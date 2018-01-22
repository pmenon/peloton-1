//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// art_index_proxy.h
//
// Identification: src/include/codegen/proxy/art_index_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "adaptive_radix_tree/Key.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "index/art_index.h"

namespace peloton {
namespace codegen {

PROXY(Key) {
  DECLARE_MEMBER(0, uint8_t *, data);
  DECLARE_MEMBER(1, uint32_t, len);
  DECLARE_MEMBER(2, uint8_t[art::Key::defaultLen], stackKey);
  DECLARE_TYPE;

  DECLARE_METHOD(set);
};

PROXY(ArtIndex) {
  DECLARE_MEMBER(0, char[sizeof(index::ArtIndex)], opaque);
  DECLARE_TYPE;

  DECLARE_METHOD(SetLoadKeyFunc);
  DECLARE_METHOD(LookupKey);
};

PROXY(ArtIndexIterator) {
  DECLARE_MEMBER(0, char[sizeof(index::ArtIndex::Iterator)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(Key, art::Key);

TYPE_BUILDER(ArtIndex, index::ArtIndex);
TYPE_BUILDER(ArtIndexIterator, index::ArtIndex::Iterator);

}  // namespace codegen
}  // namespace peloton