//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_proxy.cpp
//
// Identification: src/codegen/proxy/index_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/index_proxy.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// index::Index
//===----------------------------------------------------------------------===//

DEFINE_TYPE(Index, "index::Index", MEMBER(opaque));

}  // namespace codegen
}  // namespace peloton