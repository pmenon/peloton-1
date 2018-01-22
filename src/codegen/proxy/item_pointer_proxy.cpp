//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// item_pointer_proxy.cpp
//
// Identification: src/codegen/proxy/item_pointer_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/item_pointer_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(ItemPointer, "common::ItemPointer", MEMBER(block), MEMBER(offset));

}  // namespace codegen
}  // namespace peloton