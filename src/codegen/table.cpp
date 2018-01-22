//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table.cpp
//
// Identification: src/codegen/table.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/table.h"

#include "catalog/schema.h"
#include "codegen/proxy/data_table_proxy.h"
#include "codegen/lang/loop.h"
#include "codegen/lang/if.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/zone_map_proxy.h"
#include "index/index.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

Table::Table(storage::DataTable &table)
    : table_(table), tile_group_(*table_.GetSchema()) {
#if 0
  for (uint32_t i = 0; i < table_.GetIndexCount(); i++) {
    auto *index_meta = table_.GetIndex(i)->GetMetadata();
    indexes_.emplace_back(Index::Create(*index_meta, *this));
  }
#endif
}

llvm::Value *Table::GetIndexWithOid(CodeGen &codegen, llvm::Value *table_ptr,
                                    uint32_t index_oid) const {
  return codegen.Call(RuntimeFunctionsProxy::GetIndexByOid,
                      {table_ptr, codegen.Const32(index_oid)});
}

llvm::Type *Table::GetTableType(CodeGen &codegen) const {
  return DataTableProxy::GetType(codegen);
}

std::string Table::GetName() const { return table_.GetName(); }

llvm::Value *Table::GetTileGroupCount(CodeGen &codegen,
                                      llvm::Value *table_ptr) const {
  return codegen.Call(DataTableProxy::GetTileGroupCount, {table_ptr});
}

llvm::Value *Table::GetTileGroup(CodeGen &codegen, llvm::Value *table_ptr,
                                 llvm::Value *tile_group_id) const {
  return codegen.Call(RuntimeFunctionsProxy::GetTileGroup,
                      {table_ptr, tile_group_id});
}

llvm::Value *Table::GetZoneMapManager(CodeGen &codegen) const {
  return codegen.Call(ZoneMapManagerProxy::GetInstance, {});
}

void Table::LoadRow(CodeGen &codegen, llvm::Value *table_ptr,
                    ItemPointerAccess &item_pointer,
                    const std::vector<oid_t> &col_ids,
                    std::vector<codegen::Value> &col_vals) const {
  // Get the tile group where the row is
  auto *block = item_pointer.GetBlock(codegen);
  auto *tg_offset = codegen->CreateZExt(block, codegen.Int64Type());
  llvm::Value *tile_group = GetTileGroup(codegen, table_ptr, tg_offset);

  // Load the requested columns for the row at the given offset
  tile_group_.LoadRow(codegen, tile_group, item_pointer, col_ids, col_vals);
}

// Generate a scan over all tile groups.
//
// @code
//
// PredicateInfo predicate_arr[num_predicates]
//
// num_tile_groups = GetTileGroupCount(table_ptr)
// for (oid_t tile_group_idx = 0; tile_group_idx < num_tile_groups;
//      ++tile_group_idx) {
//   if (ShouldScanTileGroup(predicate_array, tile_group_idx)) {
//      tile_group_ptr = GetTileGroup(table_ptr, tile_group_idx)
//      consumer.TileGroupStart(tile_group_ptr);
//      tile_group.TidScan(tile_group_ptr, batch_size, consumer);
//      consumer.TileGroupEnd(tile_group_ptr);
//   }
// }
//
// @endcode
void Table::GenerateScan(CodeGen &codegen, llvm::Value *table_ptr,
                         uint32_t batch_size, ScanCallback &consumer,
                         llvm::Value *predicate_ptr,
                         size_t num_predicates) const {
  // Allocate some space for the parsed predicates (if need be!)
  llvm::Value *predicate_array =
      codegen.NullPtr(PredicateInfoProxy::GetType(codegen)->getPointerTo());
  if (num_predicates != 0) {
    predicate_array = codegen.AllocateBuffer(
        PredicateInfoProxy::GetType(codegen), num_predicates, "predicateInfo");
    codegen.Call(RuntimeFunctionsProxy::FillPredicateArray,
                 {predicate_ptr, predicate_array});
  }

  // Get the number of tile groups in the given table
  llvm::Value *tile_group_idx = codegen.Const64(0);
  llvm::Value *num_tile_groups = GetTileGroupCount(codegen, table_ptr);

  lang::Loop loop{codegen,
                  codegen->CreateICmpULT(tile_group_idx, num_tile_groups),
                  {{"tileGroupIdx", tile_group_idx}}};
  {
    // Get the tile group with the given tile group ID
    tile_group_idx = loop.GetLoopVar(0);
    llvm::Value *tile_group_ptr =
        GetTileGroup(codegen, table_ptr, tile_group_idx);
    llvm::Value *tile_group_id =
        tile_group_.GetTileGroupId(codegen, tile_group_ptr);

    // Check zone map
    llvm::Value *cond = codegen.Call(
        ZoneMapManagerProxy::ShouldScanTileGroup,
        {GetZoneMapManager(codegen), predicate_array,
         codegen.Const32(num_predicates), table_ptr, tile_group_idx});

    codegen::lang::If should_scan_tilegroup{codegen, cond};
    {
      // Inform the consumer that we're starting iteration over the tile group
      consumer.TileGroupStart(codegen, tile_group_id, tile_group_ptr);

      // Generate the scan cover over the given tile group
      tile_group_.GenerateTidScan(codegen, tile_group_ptr, batch_size,
                                  consumer);

      // Inform the consumer that we've finished iteration over the tile group
      consumer.TileGroupFinish(codegen, tile_group_ptr);
    }
    should_scan_tilegroup.EndIf();

    // Move to next tile group in the table
    tile_group_idx = codegen->CreateAdd(tile_group_idx, codegen.Const64(1));
    loop.LoopEnd(codegen->CreateICmpULT(tile_group_idx, num_tile_groups),
                 {tile_group_idx});
  }
}

}  // namespace codegen
}  // namespace peloton
