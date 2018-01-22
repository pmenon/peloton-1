//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index.h
//
// Identification: src/include/codegen/index/index.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

namespace llvm {
class Type;
class Value;
}  // namespace llvm

namespace peloton {

namespace index {
class IndexMetadata;
}  // namespace index

namespace codegen {

class CodeContext;
class CodeGen;
class Table;

//===----------------------------------------------------------------------===//
// This base class facilitates access to the main Peloton index data structures
// from codegen. All index types (i.e., SkipList, BwTree etc.) subclass this
// base class, implementing code to interact with the index.
//===----------------------------------------------------------------------===//
class Index {
 public:
  /// Constructor
  Index(const index::IndexMetadata &index_meta, Table &table);

  /// Destructor
  virtual ~Index() = default;

  /// Generate index access code in the provided code context
  virtual void GenerateCode(CodeContext &context) = 0;

  /// Perform any initialization logic
  virtual void Init(CodeGen &codegen, llvm::Value *table_ptr) const = 0;

  /// LLVM type of this index and an iterator over the index
  virtual llvm::Type *GetType(CodeGen &codegen) const = 0;
  virtual llvm::Type *GetIteratorType(CodeGen &codegen) const = 0;

  /// Metadata accessor
  const index::IndexMetadata &GetMetadata() const { return index_meta_; }

  /// Get the table the index is on
  Table &GetTable() const { return table_; }

 public:
  /// Static factory method to a create a codegen index
  static std::unique_ptr<Index> Create(
      const index::IndexMetadata &index_metadata, Table &table);

 private:
  // Index metadata
  const index::IndexMetadata &index_meta_;

  // Codegen table instance this index belongs to
  Table &table_;
};

}  // namespace codegen
}  // namespace peloton