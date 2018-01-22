//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group.cpp
//
// Identification: src/codegen/tile_group.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/tile_group.h"

#include "catalog/schema.h"
#include "codegen/lang/vectorized_loop.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/tile_group_proxy.h"
#include "codegen/scan_callback.h"
#include "codegen/type/boolean_type.h"
#include "codegen/varlen.h"

namespace peloton {
namespace codegen {

TileGroup::TileGroup(const catalog::Schema &schema) : schema_(schema) {}

void TileGroup::LoadRow(CodeGen &codegen, llvm::Value *tile_group_ptr,
                        ItemPointerAccess &item_pointer,
                        const std::vector<oid_t> &col_ids,
                        std::vector<codegen::Value> &col_vals) const {
  auto num_cols = static_cast<uint32_t>(schema_.GetColumnCount());
  auto *col_layout_type = ColumnLayoutInfoProxy::GetType(codegen);
  auto *layout_arr =
      codegen.AllocateBuffer(col_layout_type, num_cols, "columnLayout");

  // Get the column layouts
  auto col_layouts = GetColumnLayouts(codegen, tile_group_ptr, layout_arr);

  // Row
  TileGroupAccess tile_group_access{*this, col_layouts};
  auto row = tile_group_access.GetRow(item_pointer.GetOffset(codegen));
  for (auto col_id : col_ids) {
    col_vals.emplace_back(row.LoadColumn(codegen, col_id));
  }
}

// This method generates code to scan over all the tuples in the provided tile
// group. The fourth argument is allocated stack space where ColumnLayoutInfo
// structs are - we use this to acquire column layout information of this
// tile group.
//
// @code
// col_layouts := GetColumnLayouts(tile_group_ptr, column_layouts)
// num_tuples := GetNumTuples(tile_group_ptr)
//
// for (start := 0; start < num_tuples; start += vector_size) {
//   end := min(start + vector_size, num_tuples)
//   ProcessTuples(start, end, tile_group_ptr);
// }
// @endcode
//
void TileGroup::GenerateTidScan(CodeGen &codegen, llvm::Value *tile_group_ptr,
                                uint32_t batch_size,
                                ScanCallback &consumer) const {
  // Allocate some space for the column layouts
  const auto num_columns = static_cast<uint32_t>(schema_.GetColumnCount());
  llvm::Value *column_layouts = codegen.AllocateBuffer(
      ColumnLayoutInfoProxy::GetType(codegen), num_columns, "columnLayout");

  // Get the column layouts
  auto col_layouts = GetColumnLayouts(codegen, tile_group_ptr, column_layouts);

  llvm::Value *num_tuples = GetNumTuples(codegen, tile_group_ptr);
  lang::VectorizedLoop loop{codegen, num_tuples, batch_size, {}};
  {
    lang::VectorizedLoop::Range curr_range = loop.GetCurrentRange();

    // Pass the vector to the consumer
    TileGroupAccess tile_group_access{*this, col_layouts};
    consumer.ProcessTuples(codegen, curr_range.start, curr_range.end,
                           tile_group_access);

    loop.LoopEnd(codegen, {});
  }
}

llvm::Value *TileGroup::GetNumTuples(CodeGen &codegen,
                                     llvm::Value *tile_group) const {
  return codegen.Call(TileGroupProxy::GetNextTupleSlot, {tile_group});
}

llvm::Value *TileGroup::GetTileGroupId(CodeGen &codegen,
                                       llvm::Value *tile_group) const {
  return codegen.Call(TileGroupProxy::GetTileGroupId, {tile_group});
}

//===----------------------------------------------------------------------===//
// Here, we discover the layout of every column that will be accessed. A
// column's layout includes three pieces of information:
//
// 1. The starting memory address (where the first value of the column is)
// 2. The stride length
// 3. Whether the column is in columnar layout
//===----------------------------------------------------------------------===//
std::vector<TileGroup::ColumnLayout> TileGroup::GetColumnLayouts(
    CodeGen &codegen, llvm::Value *tile_group_ptr,
    llvm::Value *column_layout_infos) const {
  // Call RuntimeFunctions::GetTileGroupLayout()
  auto num_cols = static_cast<uint32_t>(schema_.GetColumnCount());
  codegen.Call(
      RuntimeFunctionsProxy::GetTileGroupLayout,
      {tile_group_ptr, column_layout_infos, codegen.Const32(num_cols)});

  // Collect <start, stride, is_columnar> triplets of all columns
  std::vector<TileGroup::ColumnLayout> layouts;
  auto *layout_type = ColumnLayoutInfoProxy::GetType(codegen);
  for (uint32_t col_id = 0; col_id < num_cols; col_id++) {
    auto *start = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        layout_type, column_layout_infos, col_id, 0));
    auto *stride = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        layout_type, column_layout_infos, col_id, 1));
    auto *columnar = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        layout_type, column_layout_infos, col_id, 2));
    layouts.push_back(ColumnLayout{col_id, start, stride, columnar});
  }
  return layouts;
}

// Load a given column for the row with the given offset
codegen::Value TileGroup::LoadColumn(
    CodeGen &codegen, llvm::Value *offset,
    const TileGroup::ColumnLayout &layout) const {
  // We're calculating: col[tid] = col_start + (tid * col_stride)
  llvm::Value *jump = codegen->CreateMul(offset, layout.stride);
  llvm::Value *col_address =
      codegen->CreateInBoundsGEP(codegen.ByteType(), layout.start_ptr, jump);

  // The value, length and is_null check
  llvm::Value *val = nullptr, *length = nullptr, *is_null = nullptr;

  // Column metadata
  bool is_nullable = schema_.AllowNull(layout.col_id);
  const auto &column = schema_.GetColumn(layout.col_id);
  const auto &sql_type = type::SqlType::LookupType(column.GetType());

  // Check if it's a string or numeric value
  if (sql_type.IsVariableLength()) {
    auto *varlen_type = VarlenProxy::GetType(codegen);
    auto *varlen_ptr_ptr = codegen->CreateBitCast(
        col_address, varlen_type->getPointerTo()->getPointerTo());
    if (is_nullable) {
      codegen::Varlen::GetPtrAndLength(
          codegen, codegen->CreateLoad(varlen_ptr_ptr), val, length, is_null);
    } else {
      codegen::Varlen::SafeGetPtrAndLength(
          codegen, codegen->CreateLoad(varlen_ptr_ptr), val, length);
    }
    PL_ASSERT(val != nullptr && length != nullptr);
  } else {
    // Get the LLVM type of the column
    llvm::Type *col_type = nullptr, *col_len_type = nullptr;
    sql_type.GetTypeForMaterialization(codegen, col_type, col_len_type);
    PL_ASSERT(col_type != nullptr && col_len_type == nullptr);

    // val = *(col_type*)col_address;
    col_address = codegen->CreateBitCast(col_address, col_type->getPointerTo());
    val = codegen->CreateLoad(col_type, col_address);

    if (is_nullable) {
      // To check for NULL, we need to perform a comparison between the value we
      // just read from the table with the NULL value for the column's type. We
      // need to be careful that the runtime type of both values is not NULL to
      // bypass the type system's NULL checking logic.
      if (sql_type.TypeId() == peloton::type::TypeId::BOOLEAN) {
        is_null = type::Boolean::Instance().CheckNull(codegen, col_address);
      } else {
        auto val_tmp = codegen::Value{sql_type, val};
        auto null_val =
            codegen::Value{sql_type, sql_type.GetNullValue(codegen).GetValue()};
        auto val_is_null = val_tmp.CompareEq(codegen, null_val);
        PL_ASSERT(!val_is_null.IsNullable());
        is_null = val_is_null.GetValue();
      }
    }
  }

  // Names
  val->setName(column.GetName());
  if (length != nullptr) length->setName(column.GetName() + ".len");
  if (is_null != nullptr) is_null->setName(column.GetName() + ".null");

  // Return the value
  auto type = type::Type{column.GetType(), is_nullable};
  return codegen::Value{type, val, length, is_null};
}

////////////////////////////////////////////////////////////////////////////////
///
/// TileGroupAccess
///
////////////////////////////////////////////////////////////////////////////////

TileGroup::TileGroupAccess::TileGroupAccess(
    const TileGroup &tile_group,
    const std::vector<TileGroup::ColumnLayout> &tile_group_layout)
    : tile_group_(tile_group), layout_(tile_group_layout) {}

TileGroup::TileGroupAccess::Row TileGroup::TileGroupAccess::GetRow(
    llvm::Value *offset) const {
  return TileGroup::TileGroupAccess::Row{*this, offset};
}

////////////////////////////////////////////////////////////////////////////////
///
/// Row
///
////////////////////////////////////////////////////////////////////////////////

TileGroup::TileGroupAccess::Row::Row(const TileGroupAccess &tg_access,
                                     llvm::Value *offset)
    : tg_access_(tg_access), offset_(offset) {}

codegen::Value TileGroup::TileGroupAccess::Row::LoadColumn(
    CodeGen &codegen, uint32_t col_idx) const {
  const auto &tile_group = tg_access_.GetTileGroup();
  const auto &layout = tg_access_.GetLayout();

  // Sanity check
  PL_ASSERT(col_idx < layout.size());

  // Load this row's column's value
  return tile_group.LoadColumn(codegen, GetOffset(), layout[col_idx]);
}

}  // namespace codegen
}  // namespace peloton
