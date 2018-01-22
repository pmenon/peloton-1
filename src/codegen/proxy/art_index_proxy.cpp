//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// art_index_proxy.cpp
//
// Identification: src/codegen/proxy/art_index_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/art_index_proxy.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// art::Key
//===----------------------------------------------------------------------===//

DEFINE_TYPE(Key, "art::ArtKey", MEMBER(data), MEMBER(len), MEMBER(stackKey));

DEFINE_METHOD(::art, Key, set);

//===----------------------------------------------------------------------===//
// index::ArtIndex
//===----------------------------------------------------------------------===//

DEFINE_TYPE(ArtIndex, "index::ArtIndex", MEMBER(opaque));
DEFINE_TYPE(ArtIndexIterator, "index::ArtIndex::Iterator", MEMBER(opaque));

DEFINE_METHOD(peloton::index, ArtIndex, SetLoadKeyFunc);

}  // namespace codegen
}  // namespace peloton