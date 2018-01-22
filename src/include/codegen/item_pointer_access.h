//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// item_pointer_access.h
//
// Identification: src/include/codegen/item_pointer_access.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/item_pointer_proxy.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Helper class to simplify interacting with ItemPointers from codegen.
//===----------------------------------------------------------------------===//
class ItemPointerAccess {
 public:
  explicit ItemPointerAccess(llvm::Value *item_pointer);

  /// Get the block the item refers to
  llvm::Value *GetBlock(CodeGen &codegen);

  /// Get the offset the item refers to
  llvm::Value *GetOffset(CodeGen &codegen);

  /// Return the LLVM type of an ItemPointer
  static llvm::Type *GetType(CodeGen &codegen);

 private:
  // An ItemPointer*
  llvm::Value *item_pointer_;
  // Cache of the block and offset
  llvm::Value *block_;
  llvm::Value *offset_;
};

////////////////////////////////////////////////////////////////////////////////
//
// Implementation below
//
////////////////////////////////////////////////////////////////////////////////

inline ItemPointerAccess::ItemPointerAccess(llvm::Value *item_pointer)
    : item_pointer_(item_pointer), block_(nullptr), offset_(nullptr) {}

inline llvm::Value *ItemPointerAccess::GetBlock(CodeGen &codegen) {
  if (block_ == nullptr) {
    // Block pointer hasn't been loaded, do so now.
    auto *block_ptr = codegen->CreateConstInBoundsGEP2_32(GetType(codegen),
                                                          item_pointer_, 0, 0);
    block_ = codegen->CreateLoad(block_ptr, "block");
  }

  // Return block
  return block_;
}

inline llvm::Value *ItemPointerAccess::GetOffset(CodeGen &codegen) {
  if (offset_ == nullptr) {
    // Offset pointer hasn't been loaded, do so now.
    auto *offset_ptr = codegen->CreateConstInBoundsGEP2_32(GetType(codegen),
                                                           item_pointer_, 0, 1);
    offset_ = codegen->CreateLoad(offset_ptr, "offset");
  }

  // Return offset
  return offset_;
}

inline llvm::Type *ItemPointerAccess::GetType(CodeGen &codegen) {
  return ItemPointerProxy::GetType(codegen);
}

}  // namespace codegen
}  // namespace peloton