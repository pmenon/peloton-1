//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table_test.cpp
//
// Identification: test/codegen/hash_table_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdlib>
#include <iosfwd>
#include <random>

#include <boost/functional.hpp>
#include "murmur3/MurmurHash3.h"

#include "codegen/runtime_functions.h"
#include "codegen/util/hash_table.h"
#include "common/harness.h"
#include "common/timer.h"

namespace peloton {
namespace test {

/**
 * The test key used in the hash table
 */
struct Key {
  uint32_t k1, k2;

  Key() = default;
  Key(uint32_t _k1, uint32_t _k2) : k1(_k1), k2(_k2) {}

  bool operator==(const Key &rhs) const { return k1 == rhs.k1 && k2 == rhs.k2; }
  bool operator!=(const Key &rhs) const { return !(rhs == *this); }

  uint64_t Hash() const {
    static constexpr uint32_t seed = 12345;
    uint64_t h1 = MurmurHash3_x86_32(&k1, sizeof(uint32_t), seed);
    uint64_t h2 = MurmurHash3_x86_32(&k2, sizeof(uint32_t), seed);
    return h1 * 0x9ddfea08eb382d69ull * h2;
  }

  friend std::ostream &operator<<(std::ostream &os, const Key &k) {
    os << "Key[" << k.k1 << "," << k.k2 << "]";
    return os;
  }
};

/**
 * The test value used in the hash table
 */
struct Value {
  uint32_t v1, v2, v3, v4;
  bool operator==(const Value &rhs) const {
    return v1 == rhs.v1 && v2 == rhs.v2 && v3 == rhs.v3 && v4 == rhs.v4;
  }
  bool operator!=(const Value &rhs) const { return !(rhs == *this); }
};

/**
 * The base hash table test class
 */
class HashTableTest : public PelotonTest {
 public:
  HashTableTest() : pool_(new ::peloton::type::EphemeralPool()) {}

  type::AbstractPool &GetMemPool() const { return *pool_; }

 private:
  std::unique_ptr<::peloton::type::AbstractPool> pool_;
};

TEST_F(HashTableTest, UniqueKeysTest) {
  codegen::util::HashTable table(GetMemPool(), sizeof(Key), sizeof(Value));

  constexpr uint32_t to_insert = 50000;
  constexpr uint32_t c1 = 4444;

  std::vector<Key> keys;

  // Insert keys
  for (uint32_t i = 0; i < to_insert; i++) {
    Key k{1, i};
    Value v = {.v1 = k.k2, .v2 = 2, .v3 = 3, .v4 = c1};
    table.TypedInsert(k.Hash(), k, v);

    keys.emplace_back(k);
  }

  EXPECT_EQ(to_insert, table.NumElements());

  // Lookup
  for (const auto &key : keys) {
    uint32_t count = 0;
    std::function<void(const Value &v)> f = [&key, &count,
                                             &c1](const Value &v) {
      EXPECT_EQ(key.k2, v.v1)
          << "Value's [v1] found in table doesn't match insert key";
      EXPECT_EQ(c1, v.v4) << "Value's [v4] doesn't match constant";
      count++;
    };
    table.TypedProbe(key.Hash(), key, f);
    EXPECT_EQ(1, count) << "Found duplicate keys in unique key test";
  }
}

TEST_F(HashTableTest, BuildEmptyHashTable) {
  codegen::util::HashTable table{GetMemPool(), sizeof(Key), sizeof(Value)};

  std::vector<Key> keys;

  // Insert keys
  for (uint32_t i = 0; i < 10; i++) {
    Key k{1, i};
    // do NOT insert into hash table

    keys.emplace_back(k);
  }

  EXPECT_EQ(0, table.NumElements());

  // Build lazy
  table.BuildLazy();

  // Lookups should succeed
  for (const auto &key : keys) {
    std::function<void(const Value &v)> f =
        [](UNUSED_ATTRIBUTE const Value &v) {};
    auto ret = table.TypedProbe(key.Hash(), key, f);
    EXPECT_EQ(false, ret);
  }
}

TEST_F(HashTableTest, DuplicateKeysTest) {
  codegen::util::HashTable table(GetMemPool(), sizeof(Key), sizeof(Value));

  constexpr uint32_t to_insert = 50000;
  constexpr uint32_t c1 = 4444;
  constexpr uint32_t max_dups = 4;

  std::vector<Key> keys;

  // Insert keys
  uint32_t num_inserts = 0;
  for (uint32_t i = 0; i < to_insert; i++) {
    // Choose a random number of duplicates to insert. Store this in the k1.
    uint32_t num_dups = 1 + (rand() % max_dups);
    Key k{num_dups, i};

    // Duplicate insertion
    for (uint32_t dup = 0; dup < num_dups; dup++) {
      Value v = {.v1 = k.k2, .v2 = 2, .v3 = 3, .v4 = c1};
      table.TypedInsert(k.Hash(), k, v);
      num_inserts++;
    }

    keys.emplace_back(k);
  }

  EXPECT_EQ(num_inserts, table.NumElements());

  // Lookup
  for (const auto &key : keys) {
    uint32_t count = 0;
    std::function<void(const Value &v)> f = [&key, &count,
                                             &c1](const Value &v) {
      EXPECT_EQ(key.k2, v.v1)
          << "Value's [v1] found in table doesn't match insert key";
      EXPECT_EQ(c1, v.v4) << "Value's [v4] doesn't match constant";
      count++;
    };
    table.TypedProbe(key.Hash(), key, f);
    EXPECT_EQ(key.k1, count) << key << " found " << count << " dups ...";
  }
}

TEST_F(HashTableTest, LazyInsertsWithDupsTest) {
  codegen::util::HashTable table{GetMemPool(), sizeof(Key), sizeof(Value)};

  constexpr uint32_t to_insert = 50000;
  constexpr uint32_t c1 = 4444;
  constexpr uint32_t max_dups = 4;

  std::vector<Key> keys;

  // Insert keys
  uint32_t num_inserts = 0;
  for (uint32_t i = 0; i < to_insert; i++) {
    // Choose a random number of duplicates to insert. Store this in the k1.
    uint32_t num_dups = 1 + (rand() % max_dups);
    Key k{num_dups, i};

    // Duplicate insertion
    for (uint32_t dup = 0; dup < num_dups; dup++) {
      Value v = {.v1 = k.k2, .v2 = 2, .v3 = 3, .v4 = c1};
      table.TypedInsertLazy(k.Hash(), k, v);
      num_inserts++;
    }

    keys.emplace_back(k);
  }

  // Number of elements should reflect lazy insertions
  EXPECT_EQ(num_inserts, table.NumElements());
  EXPECT_LT(table.Capacity(), table.NumElements());

  // Build lazy
  table.BuildLazy();

  // Lookups should succeed
  for (const auto &key : keys) {
    uint32_t count = 0;
    std::function<void(const Value &v)> f = [&key, &count,
                                             &c1](const Value &v) {
      EXPECT_EQ(key.k2, v.v1)
          << "Value's [v1] found in table doesn't match insert key";
      EXPECT_EQ(c1, v.v4) << "Value's [v4] doesn't match constant";
      count++;
    };
    table.TypedProbe(key.Hash(), key, f);
    EXPECT_EQ(key.k1, count) << key << " found " << count << " dups ...";
  }
}

TEST_F(HashTableTest, VectorizedScanTest) {
  ////////////////////////////////////////////////////////////////////
  ///
  /// First, a normal test. Insert a bunch of keys, iterate and ensure
  /// everything exists.
  ///
  ////////////////////////////////////////////////////////////////////
  {
    codegen::util::HashTable table(GetMemPool(), sizeof(Key), sizeof(Value));

    const uint32_t num_keys = 100000;

    struct KeyHasher {
      size_t operator()(const Key &k) const { return k.Hash(); }
    };
    std::unordered_map<Key, Value, KeyHasher> ref_map;

    for (uint32_t i = 0; i < num_keys; i++) {
      Key k(i, 0);
      Value v = {0, 1, 2, 3};
      table.TypedInsert(k.Hash(), k, v);
      ref_map.insert(std::make_pair(k, v));
    }

    constexpr uint32_t vec_size = 1024;
    uint32_t selection_vector[vec_size] = {0};
    codegen::util::HashTable::ScanState scan(table, selection_vector, vec_size);

    // Make sure the scanner is invalid until the first call to Next()
    EXPECT_EQ(0, scan.CurrentBatchSize());

    scan.Next();

    EXPECT_GT(scan.CurrentBatchSize(), 0);

    uint32_t entry_count = 0;
    do {
      auto *entries = scan.Entries();
      for (uint32_t i = 0; i < scan.CurrentBatchSize(); i++) {
        const Key *k;
        const Value *v;
        entries[i]->GetKV(k, v);

        // Check in reference map
        auto iter = ref_map.find(*k);
        EXPECT_FALSE(iter == ref_map.end());
        EXPECT_EQ(iter->second, *v);
        entry_count++;
      }
    } while (!scan.Next());

    // Make sure the counts are the same
    EXPECT_EQ(num_keys, entry_count);
  }

  ////////////////////////////////////////////////////////////////////
  ///
  /// In this sub-test, we want to test that the iteration can correctly
  /// straddle between buckets. To do this, we construct a table such
  /// each bucket chain has a size 1/4 of the desired vector size. Each
  /// invocation of Next() will require moving between four buckets.
  ///
  ////////////////////////////////////////////////////////////////////
  {
    codegen::util::HashTable table(GetMemPool(), sizeof(Key), sizeof(Value));

    constexpr uint32_t vec_size = 512;

    constexpr uint32_t num_buckets_to_insert_into = 32;

    struct KeyHasher {
      size_t operator()(const Key &k) const { return k.Hash(); }
    };
    std::unordered_map<Key, Value, KeyHasher> ref_map;

    uint32_t num_keys = 0;
    for (uint32_t bucket = 0; bucket < num_buckets_to_insert_into; bucket++) {
      for (uint32_t i = 0; i < vec_size / 4; i++) {
        Key k(bucket, 0);
        Value v = {0, 1, 2, 3};
        table.TypedInsert(k.Hash(), k, v);
        num_keys++;
      }
    }

    EXPECT_EQ(num_keys, table.NumElements());

    uint32_t selection_vector[vec_size] = {0};
    codegen::util::HashTable::ScanState scan(table, selection_vector, vec_size);

    uint32_t entry_count = 0;
    do {
      auto *entries = scan.Entries();
      for (uint32_t i = 0; i < scan.CurrentBatchSize(); i++) {
        const Key *k;
        const Value *v;
        entries[i]->GetKV(k, v);
        entry_count++;
      }
    } while (!scan.Next());

    // Make sure the counts are the same
    EXPECT_EQ(num_keys, entry_count);
  }
}

TEST_F(HashTableTest, ParallelMergeTest) {
  constexpr uint32_t num_threads = 4;
  constexpr uint32_t to_insert = 20000;

  // Allocate hash tables for each thread
  executor::ExecutorContext exec_ctx{nullptr};

  auto &thread_states = exec_ctx.GetThreadStates();
  thread_states.Reset(sizeof(codegen::util::HashTable));
  thread_states.Allocate(num_threads);

  // The keys we insert
  std::mutex keys_mutex;
  std::vector<Key> keys;

  // The global hash table
  codegen::util::HashTable global_table(*exec_ctx.GetPool(), sizeof(Key),
                                        sizeof(Value));

  auto add_key = [&keys_mutex, &keys](const Key &k) {
    std::lock_guard<std::mutex> lock{keys_mutex};
    keys.emplace_back(k);
  };

  ////////////////////////////////////////////////////////////////////
  /// Insert data in parallel into thread-local hash tables
  ////////////////////////////////////////////////////////////////////
  {
    auto insert_fn = [&add_key, &exec_ctx](uint64_t tid) {
      // Get the local table for this thread
      auto &thread_states = exec_ctx.GetThreadStates();
      auto *table =
          thread_states.AccessThreadStateAs<codegen::util::HashTable>(tid);

      // Initialize it
      codegen::util::HashTable::Init(*table, exec_ctx, sizeof(Key),
                                     sizeof(Value));

      // Insert keys disjoint from other threads
      for (uint32_t i = tid * to_insert, end = i + to_insert; i != end; i++) {
        Key key(static_cast<uint32_t>(tid), i);
        Value value = {.v1 = key.k2, .v2 = key.k1, .v3 = 3, .v4 = 4444};
        table->TypedInsertLazy(key.Hash(), key, value);

        add_key(key);
      }
    };

    // Launch
    LaunchParallelTest(num_threads, insert_fn);

    // Check
    for (uint32_t tid = 0; tid < num_threads; tid++) {
      auto *ht = reinterpret_cast<codegen::util::HashTable *>(
          thread_states.AccessThreadState(tid));
      EXPECT_EQ(to_insert, ht->NumElements());
    }
  }

  ////////////////////////////////////////////////////////////////////
  /// Resize global hash table based on thread-local sizes
  ////////////////////////////////////////////////////////////////////
  {
    global_table.ReserveLazy(thread_states, 0);
    EXPECT_EQ(NextPowerOf2(keys.size()), global_table.Capacity());
  }

  ////////////////////////////////////////////////////////////////////
  /// Merge thread-local tables into global in parallel
  ////////////////////////////////////////////////////////////////////
  {
    auto merge_fn = [&global_table, &thread_states](uint64_t tid) {
      auto *table =
          thread_states.AccessThreadStateAs<codegen::util::HashTable>(tid);
      global_table.MergeLazyUnfinished(*table);
    };

    LaunchParallelTest(num_threads, merge_fn);
  }

  ////////////////////////////////////////////////////////////////////
  /// Clean up thread-local tables
  ////////////////////////////////////////////////////////////////////
  {
    for (uint32_t tid = 0; tid < num_threads; tid++) {
      auto *table = reinterpret_cast<codegen::util::HashTable *>(
          thread_states.AccessThreadState(tid));
      codegen::util::HashTable::Destroy(*table);
    }
  }

  // Now probe global
  EXPECT_EQ(to_insert * num_threads, global_table.NumElements());
  EXPECT_LE(global_table.NumElements(), global_table.Capacity());

  for (const auto &key : keys) {
    uint32_t count = 0;
    std::function<void(const Value &v)> f = [&key, &count](const Value &v) {
      EXPECT_EQ(key.k2, v.v1)
          << "Value's [v1] found in table doesn't match insert key";
      EXPECT_EQ(key.k1, v.v2) << "Key " << key << " inserted by thread "
                              << key.k1 << " but value was inserted by thread "
                              << v.v2;
      count++;
    };
    global_table.TypedProbe(key.Hash(), key, f);
    EXPECT_EQ(1, count) << "Found duplicate keys in unique key test";
  }
}

namespace {

void PartitionedUpdateFunction(bool exists, Value *curr_val) {
  if (exists) {
    curr_val->v1++;
  } else {
    *curr_val = {1, 2, 3, 4};
  }
}

}  // namespace

TEST_F(HashTableTest, InsertPartitionedTest) {
  codegen::util::HashTable table(GetMemPool(), sizeof(Key), sizeof(Value));

  const uint32_t num_groups = static_cast<uint32_t>(table.FlushThreshold() + 9);
  const uint32_t to_insert = 50000;

  std::vector<Key> keys;

  /// Generate test data
  for (uint32_t i = 0; i < to_insert; i++) {
    keys.emplace_back(i % num_groups, i % num_groups);
  }

  /// Insert partitioned
  for (uint32_t i = 0; i < to_insert; i++) {
    table.TypedUpsert<true, Key, Value>(
        keys[i].Hash(), keys[i], [](bool exists, Value *curr) {
          PartitionedUpdateFunction(exists, curr);
        });
  }

  // We should have flushed at least once
  EXPECT_TRUE(table.NumFlushes() > 0);

  // We can never have more elements than # groups
  EXPECT_FALSE(table.NumElements() > num_groups);
}

#if 0
TEST_F(HashTableTest, ParallelPartitionedBuild) {
  constexpr uint32_t num_threads = 4;

  // Vary groups between 10, 100, 1000
  const uint32_t num_keys = 100000;
  for (const uint32_t num_groups : {10u, 100u, 1000u}) {
    executor::ExecutorContext ctx(nullptr);
    codegen::util::HashTable global_table(*ctx.GetPool(), sizeof(Key),
                                          sizeof(Value));

    // Setup thread state for "num_threads"
    ctx.GetThreadStates().Reset(sizeof(codegen::util::HashTable));
    ctx.GetThreadStates().Allocate(num_threads);

    //////////////////////////////////////////////////////////////////
    /// Do parallel (partitioned) inserts into thread-local tables
    //////////////////////////////////////////////////////////////////
    {
      auto fn = [&num_threads, &num_groups, &ctx](uint64_t tid) {
        // First, initialize thread-local table
        auto *table =
            ctx.GetThreadStates().AccessThreadStateAs<codegen::util::HashTable>(
                tid);
        codegen::util::HashTable::Init(*table, ctx, sizeof(Key), sizeof(Value));

        // Insert
        for (uint32_t i = 0; i < num_keys / num_threads; i++) {
          Key group_key = {i % num_groups, i % num_groups};
          table->TypedUpsert<true, Key, Value>(
              group_key.Hash(), group_key, [](bool exists, Value *curr) {
                PartitionedUpdateFunction(exists, curr);
              });
        }
      };

      // Launch thread-local population in parallel
      LaunchParallelTest(num_threads, fn);
    }

    //////////////////////////////////////////////////////////////////
    /// Transfer partitions to global table
    //////////////////////////////////////////////////////////////////
    {
      auto merge_fn = [](codegen::util::HashTable &table,
                         codegen::util::HashTable::Entry **partitions,
                         uint64_t part_begin, uint64_t part_end) {
        (void)table;
        for (uint64_t pid = part_begin; pid < part_end; pid++) {
          auto *entry = partitions[pid];
          while (entry != nullptr) {
          }
        }
      };

      // The "0" is for the offset of the hash table in the state. It's 0
      // because it's the only element in the state and it's situated in the
      // first slot.
      global_table.TransferPartitions(ctx.GetThreadStates(), 0, merge_fn);
    }

    //////////////////////////////////////////////////////////////////
    /// Final scan to check results
    //////////////////////////////////////////////////////////////////
    {
      std::mutex mutex;
      auto scan_fn = [](void *query_state, void *thread_state,
                        const codegen::util::HashTable &table) {
        (void)thread_state;
        (void)table;
        std::mutex *m = reinterpret_cast<std::mutex *>(query_state);
        (void)m;

      };
      global_table.ExecutePartitionedScan(static_cast<void *>(&mutex),
                                          ctx.GetThreadStates(), scan_fn);
    }
  }
}
#endif

}  // namespace test
}  // namespace peloton