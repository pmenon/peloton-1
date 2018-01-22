//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table.h
//
// Identification: src/include/codegen/table.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/generated_function.h"
#include "codegen/index/index.h"
#include "codegen/item_pointer_access.h"
#include "codegen/scan_callback.h"
#include "codegen/tile_group.h"

namespace peloton {

namespace storage {
class DataTable;
}  // namespace storage

namespace codegen {

//===----------------------------------------------------------------------===//
//
// This class the main entry point for any code generation that requires
// operating on physical tables. Ideally, there should only be one instance of
// this for every table in the database. This means some form of catalog should
// exist for such tables. Or, every storage::Table will have an instance of this
// "generator" class.
//
//===----------------------------------------------------------------------===//
class Table {
 public:
  /// Constructor
  explicit Table(storage::DataTable &table);

  /// Get the index with the OID from the this table
  llvm::Value *GetIndexWithOid(CodeGen &codegen, llvm::Value *table_ptr,
                               uint32_t index_oid) const;

  /// Get the LLVM type of this table
  llvm::Type *GetTableType(CodeGen &codegen) const;

  /// Get the name of the table
  std::string GetName() const;

  /// Load the row pointed to by the provided item pointer
  void LoadRow(CodeGen &codegen, llvm::Value *table_ptr,
               ItemPointerAccess &item_pointer,
               const std::vector<oid_t> &col_ids,
               std::vector<codegen::Value> &col_vals) const;

  /// Generate all table logic code
  void GenerateCode(CodeContext &cc);

  /// Prepare the table for execution
  void PrepareForExecution(CodeContext &cc);

  /// Generate code to perform a scan over the given table. The table pointer
  /// is provided as the second argument. The scan consumer (third argument)
  /// should be notified when ready to generate the scan loop body.
  void GenerateScan(CodeGen &codegen, llvm::Value *table_ptr,
                    uint32_t batch_size, ScanCallback &consumer,
                    llvm::Value *predicate_array, size_t num_predicates) const;

 private:
  /// Generate initialization logic
  void GenerateInit(CodeContext &cc);

  /// Given a table instance, return the number of tile groups in the table.
  llvm::Value *GetTileGroupCount(CodeGen &codegen,
                                 llvm::Value *table_ptr) const;

  /// Retrieve an instance of the provided table's tile group given the tile
  /// group's index.
  llvm::Value *GetTileGroup(CodeGen &codegen, llvm::Value *table_ptr,
                            llvm::Value *tile_group_id) const;

  llvm::Value *GetZoneMapManager(CodeGen &codegen) const;

 private:
  // The table associated with this generator
  storage::DataTable &table_;

  // The generator for a tile group
  TileGroup tile_group_;

  // The indexes in the table
  std::vector<std::unique_ptr<Index>> indexes_;

  // The generated initialization function
  std::unique_ptr<GeneratedFunction> init_func_;
};

}  // namespace codegen
}  // namespace peloton