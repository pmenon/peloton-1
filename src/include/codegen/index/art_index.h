//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// art_index.h
//
// Identification: src/include/codegen/index/art_index.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/generated_function.h"
#include "codegen/index/index.h"

namespace peloton {

namespace index {
class IndexMetadata;
}  // namespace index

namespace codegen {

class ArtIndex : public Index {
 public:
  ArtIndex(const index::IndexMetadata &index_meta, Table &table);

  /// Generate all ART-related index code
  void GenerateCode(CodeContext &cc) override;

  /// Perform any initialization work for this index
  void Init(CodeGen &codegen, llvm::Value *table_ptr) const override;

  /// Types
  llvm::Type *GetType(CodeGen &codegen) const override;
  llvm::Type *GetIteratorType(CodeGen &codegen) const override;

 private:
  /// Generate the load-key function for the index
  void GenerateLoadKeyFunction(CodeContext &cc);

  /// Generate the scan-key function
  void GenerateScanKeyFunction(CodeContext &cc);

 private:
  // The generated load-key function
  std::unique_ptr<GeneratedFunction> load_key_func_;
};

}  // namespace codegen
}  // namespace peloton