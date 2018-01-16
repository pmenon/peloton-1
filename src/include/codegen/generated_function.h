//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// generated_function.h
//
// Identification: src/include/codegen/generated_function.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/function_builder.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
//
// A generated function is one that was constructed, just-in-time compiled, and
// loaded into the database. These functions can be invoked from any CodeContext
// using Resolve(), which builds and returns an llvm::Function "wrapper" around
// the function. Thus, it allows for full type-checking during IR construction,
// and invocations will call directly into native (JITed) code.
//
//===----------------------------------------------------------------------===//
class GeneratedFunction {
 public:
  using DeclarationGenerator =
      const std::function<FunctionDeclaration(CodeContext &cc)>;

  // Constructors
  GeneratedFunction(CodeContext &source_cc, llvm::Function *func,
                    const DeclarationGenerator &declaration_generator);

  // Resolve this function in the **provided** code context
  llvm::Function *Resolve(CodeContext &target_cc) const;

 private:
  // The code context
  CodeContext &source_cc_;
  // The generated LLVM function
  llvm::Function *func_;
  // The signature of the generate function
  const DeclarationGenerator &generator_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline GeneratedFunction::GeneratedFunction(
    CodeContext &source_cc, llvm::Function *func,
    const DeclarationGenerator &declaration_generator)
    : source_cc_(source_cc), func_(func), generator_(declaration_generator) {}

inline llvm::Function *GeneratedFunction::Resolve(
    CodeContext &target_cc) const {
  if (&source_cc_ == &target_cc) {
    // The context hasn't been compiled yet
    return func_;
  }
  auto *func_impl = source_cc_.GetRawFunctionPointer(func_);
  PL_ASSERT(func_impl != nullptr);

  auto func_declaration = generator_(target_cc);
  auto *func = func_declaration.GetDeclaredFunction();
  target_cc.RegisterExternalFunction(func, func_, func_impl);
  return func;
}

}  // namespace codegen
}  // namespace peloton
