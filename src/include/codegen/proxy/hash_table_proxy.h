//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table_proxy.h
//
// Identification: src/include/codegen/proxy/hash_table_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/util/hash_table.h"

namespace peloton {
namespace codegen {

/// The proxy for util::HashTable::Entry
PROXY(Entry) {
  DECLARE_MEMBER(0, uint64_t, hash);
  DECLARE_MEMBER(1, util::HashTable::Entry *, next);
  DECLARE_TYPE;
};

/// The proxy for util::HashTable
PROXY(HashTable) {
  // clang-format off
  DECLARE_MEMBER(0, char[sizeof(void *) +                   // Memory pool
                         sizeof(uint32_t) +                 // Key size
                         sizeof(uint32_t)],                 // Value size
                 opaque_1);
  DECLARE_MEMBER(1, util::HashTable::Entry **, directory);  // Directory
  DECLARE_MEMBER(2, uint64_t, dir_size);                    // Directory size
  DECLARE_MEMBER(3, uint64_t, mask);                        // Mask
  DECLARE_MEMBER(4, char[sizeof(void *) +                   // Memory block
                         sizeof(void *) +                   // Next free entry
                         sizeof(uint64_t)],                 // Available bytes
                 opaque_2);
  DECLARE_MEMBER(5, uint64_t, num_elems);
  DECLARE_MEMBER(6, uint64_t, capacity);
  DECLARE_MEMBER(7, char[sizeof(void *) +         // Merging function
                         sizeof(void *) +         // Partition heads
                         sizeof(void *) +         // Partition tails
                         sizeof(void *) +         // Partition hash tables
                         sizeof(uint64_t) +       // Flush threshold
                         sizeof(uint64_t) +       // Partition shift bits
                         sizeof(util::HashTable::Stats)],   // Statistics
                 opaque_3);
  // clang-format on
  DECLARE_TYPE;

  // Proxy all methods that will be called from codegen
  DECLARE_METHOD(Init);
  DECLARE_METHOD(Insert);
  DECLARE_METHOD(InsertLazy);
  DECLARE_METHOD(InsertPartitioned);
  DECLARE_METHOD(TransferPartitions);
  DECLARE_METHOD(ExecutePartitionedScan);
  DECLARE_METHOD(BuildAllPartitions);
  DECLARE_METHOD(Repartition);
  DECLARE_METHOD(MergePartitions);
  DECLARE_METHOD(BuildLazy);
  DECLARE_METHOD(ReserveLazy);
  DECLARE_METHOD(MergeLazyUnfinished);
  DECLARE_METHOD(Grow);
  DECLARE_METHOD(Destroy);
};

PROXY(ScanState) {
  DECLARE_MEMBER(0, util::HashTable &, table);
  DECLARE_MEMBER(1, uint32_t *, sel);
  DECLARE_MEMBER(2, util::HashTable::Entry **, entries);
  DECLARE_MEMBER(3, uint64_t, index);
  DECLARE_MEMBER(4, util::HashTable::Entry *, next);
  DECLARE_MEMBER(5, uint32_t, size);
  DECLARE_MEMBER(6, uint32_t, sel_size);
  DECLARE_MEMBER(7, bool, done);
  DECLARE_TYPE;

  DECLARE_METHOD(Init);
  DECLARE_METHOD(Destroy);
  DECLARE_METHOD(Next);
};

/// The type builders for Entry and HashTable
TYPE_BUILDER(Entry, util::HashTable::Entry);
TYPE_BUILDER(HashTable, util::HashTable);
TYPE_BUILDER(ScanState, util::HashTable::ScanState);

}  // namespace codegen
}  // namespace peloton