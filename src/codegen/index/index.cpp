//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index.cpp
//
// Identification: src/codegen/index/index.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/index/index.h"

#include "codegen/index/art_index.h"
#include "index/index.h"

namespace peloton {
namespace codegen {

Index::Index(const index::IndexMetadata &index_meta, Table &table)
    : index_meta_(index_meta), table_(table) {}

/// Index factory method
std::unique_ptr<Index> Index::Create(const index::IndexMetadata &index_metadata,
                                     Table &table) {
  // TODO(pmenon): Fill me out
  auto index_type = index_metadata.GetIndexType();
  switch (index_type) {
    case IndexType::ART: {
      return std::unique_ptr<Index>{new ArtIndex(index_metadata, table)};
    }
    default: {
      throw Exception{
          StringUtil::Format("Index type '%s' not support in codegen ...",
                             IndexTypeToString(index_type).c_str())};
    }
  }
}

}  // namespace codegen
}  // namespace peloton