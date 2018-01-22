//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group.h
//
// Identification: src/include/codegen/tile_group.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/item_pointer_access.h"
#include "codegen/value.h"

namespace peloton {

namespace catalog {
class Schema;
}  // namespace catalog

namespace codegen {

class ScanCallback;

//===----------------------------------------------------------------------===//
//
// Like codegen::Table, this class is the main entry point for code generation
// for tile groups. An instance of this class should exist for each instance of
// codegen::Table (in fact, this should be a member variable).  The schema of
// the tile group is that of the whole table. We abstract away the fact that
// each _individual_ tile group by have a different layout.
//
//===----------------------------------------------------------------------===//
class TileGroup {
 public:
  /// Constructor
  explicit TileGroup(const catalog::Schema &schema);

  /// Load the row pointed to by the given item pointer
  void LoadRow(CodeGen &codegen, llvm::Value *tile_group_ptr,
               ItemPointerAccess &item_pointer,
               const std::vector<oid_t> &col_ids,
               std::vector<codegen::Value> &col_vals) const;

  /// Generate code that performs a sequential scan over the provided tile group
  void GenerateTidScan(CodeGen &codegen, llvm::Value *tile_group_ptr,
                       uint32_t batch_size, ScanCallback &consumer) const;

  /// Return the number of tuples in the provided tile group
  llvm::Value *GetNumTuples(CodeGen &codegen, llvm::Value *tile_group) const;

  /// Return the ID of the provided tile group
  llvm::Value *GetTileGroupId(CodeGen &codegen, llvm::Value *tile_group) const;

 private:
  // A struct to capture enough information to perform strided accesses
  struct ColumnLayout {
    uint32_t col_id;
    llvm::Value *start_ptr;
    llvm::Value *stride;
    llvm::Value *is_columnar;
  };

  std::vector<TileGroup::ColumnLayout> GetColumnLayouts(
      CodeGen &codegen, llvm::Value *tile_group_ptr,
      llvm::Value *column_layout_infos) const;

  /// Access a given column for the row with the given tid
  codegen::Value LoadColumn(CodeGen &codegen, llvm::Value *offset,
                            const TileGroup::ColumnLayout &layout) const;

 public:
  // Helper class to access tuple data in the tile group
  class TileGroupAccess {
   public:
    // A row in this tile group
    class Row {
     public:
      /// Constructor
      Row(const TileGroupAccess &tg_access, llvm::Value *offset);

      /// Load the column at the given index
      codegen::Value LoadColumn(CodeGen &codegen, uint32_t col_idx) const;

      /// Return the offset of this row in the tile group
      llvm::Value *GetOffset() const { return offset_; }

     private:
      // The tile group accessor class
      const TileGroupAccess &tg_access_;
      // The offset of this row in the tile group
      llvm::Value *offset_;
    };

    /// Constructor
    TileGroupAccess(const TileGroup &tile_group,
                    const std::vector<ColumnLayout> &tile_group_layout);

    /// Load a specific row from the batch
    Row GetRow(llvm::Value *offset) const;

    const TileGroup &GetTileGroup() const { return tile_group_; }

    const std::vector<ColumnLayout> &GetLayout() const { return layout_; }

   private:
    // The tile group
    const TileGroup &tile_group_;

    // The layout of all columns in the tile group
    const std::vector<ColumnLayout> &layout_;
  };

 private:
  // The schema for the table
  const catalog::Schema &schema_;
};

}  // namespace codegen
}  // namespace peloton
