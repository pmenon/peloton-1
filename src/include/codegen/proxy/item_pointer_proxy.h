//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// item_pointer_proxy.h
//
// Identification: src/include/codegen/proxy/item_pointer_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "common/item_pointer.h"

namespace peloton {
namespace codegen {

PROXY(ItemPointer) {
  DECLARE_MEMBER(0, oid_t, block);
  DECLARE_MEMBER(1, oid_t, offset);
  DECLARE_TYPE;
};

TYPE_BUILDER(ItemPointer, ItemPointer);

}  // namespace codegen
}  // namespace peloton
