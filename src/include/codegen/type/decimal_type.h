//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_type.h
//
// Identification: src/include/codegen/type/decimal_type.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/type/sql_type.h"
#include "codegen/type/type_system.h"
#include "common/singleton.h"

namespace peloton {
namespace codegen {
namespace type {

class Decimal : public SqlType, public Singleton<Decimal> {
 public:
  uint64_t GetMaxSize(const type::Type &type) const override;

  bool IsVariableLength() const override { return false; }

  Value GetMinValue(CodeGen &codegen) const override;

  Value GetMaxValue(CodeGen &codegen) const override;

  Value GetNullValue(CodeGen &codegen) const override;

  void GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type,
                                 llvm::Type *&len_type) const override;

  llvm::Function *GetOutputFunction(CodeGen &codegen,
                                    const Type &type) const override;

  const TypeSystem &GetTypeSystem() const override { return type_system_; }

 private:
  friend class Singleton<Decimal>;

  Decimal();

 private:
  // The boolean type's type-system
  TypeSystem type_system_;
};

}  // namespace type
}  // namespace codegen
}  // namespace peloton