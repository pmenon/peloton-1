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
    Key k(1, i);
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

  // Create keys, but do **NOT** insert into hash table
  for (uint32_t i = 0; i < 10; i++) {
    keys.emplace_back(1, i);
  }

  EXPECT_EQ(0, table.NumElements());

  // Build lazy
  table.BuildLazy();

  // **ALL** lookups should fail
  for (const auto &key : keys) {
    std::function<void(const Value &v)> f =
        [](UNUSED_ATTRIBUTE const Value &v) {};
    auto found = table.TypedProbe(key.Hash(), key, f);
    EXPECT_EQ(false, found);
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
    Key k(num_dups, i);

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
    Key k(num_dups, i);

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

TEST_F(HashTableTest, BuildAllPartitionsTest) {
  const auto benchmark = [this](uint64_t num_groups) {
    ////////////////////////////////////////////////////////
    /// This test constructs a partitioned hash table, then
    /// flushes all overflow partitions and builds a set of
    /// partitioned tables
    ////////////////////////////////////////////////////////
    codegen::util::HashTable table(GetMemPool(), sizeof(Key), sizeof(Value));

    const auto count_per_group = 1000u;
    const auto to_insert = num_groups * count_per_group;

    ////////////////////////////////////////////////////////
    /// Generate test data
    ////////////////////////////////////////////////////////
    std::vector<Key> keys;
    {
      for (uint32_t i = 0; i < to_insert; i++) {
        keys.emplace_back(i % num_groups, i % num_groups);
      }
    }

    ////////////////////////////////////////////////////////
    /// Insert partitioned
    ////////////////////////////////////////////////////////
    {
      const auto upsert = [](bool exists, Value *curr) {
        if (exists) {
          curr->v1++;  // update
        } else {
          *curr = {1, 2, 3, 4};  // initialize
        }
      };

      for (uint32_t i = 0; i < to_insert; i++) {
        table.TypedUpsert<true, Key, Value>(keys[i].Hash(), keys[i], upsert);
      }
    }

    ////////////////////////////////////////////////////////
    /// Build hash tables over partitions
    ////////////////////////////////////////////////////////
    {
      executor::ExecutorContext tmp(nullptr);
      tmp.GetThreadStates().Reset(sizeof(codegen::util::HashTable));
      tmp.GetThreadStates().Allocate(0);

      // This function merges all entries in all partitions in the range
      // [begin, end) into the target hash table
      const auto merge_func = [](void *, codegen::util::HashTable &target,
                                 codegen::util::HashTable::Entry **partitions,
                                 uint64_t begin, uint64_t end) {
        for (uint64_t i = begin; i < end; i++) {
          for (auto *e = partitions[i]; e != nullptr; e = e->next) {
            // Read key-value from the entry in the partition
            const Key *k;
            const Value *v;
            e->GetKV(k, v);

            // Update in main table
            const auto upsert = [&v](bool exists, Value *curr) {
              if (exists) {
                curr->v1 += v->v1;
              } else {
                *curr = *v;
              }
            };

            target.TypedUpsert<false, Key, Value>(k->Hash(), *k, upsert);
          }
        }
      };

      table.FlushToOverflowPartitions();

      // Set up the merging function
      table.TransferPartitions(tmp.GetThreadStates(), 0, merge_func);

      // Perform the build
      table.BuildAllPartitions(nullptr);
    }

    ////////////////////////////////////////////////////////
    /// All probes should succeed
    ////////////////////////////////////////////////////////
    {
      for (const auto &key : keys) {
        bool dups = false;
        uint32_t count = 0;
        bool found = table.TypedProbe<Key, Value, true>(
            key.Hash(), key, [&dups, &count](const Value &v) {
              EXPECT_FALSE(dups);
              count += v.v1;
              dups = true;
            });
        EXPECT_TRUE(found);
        EXPECT_EQ(count_per_group, count);
      }
    }
  };

  /// Try a bunch of different group sizes
  for (uint32_t num_groups = 4; num_groups < 1024; num_groups *= 2) {
    benchmark(num_groups);
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
      // The key/value to insert
      Key k(i, 0);
      Value v = {.v1 = 0, .v2 = 1, .v3 = 2, .v4 = 3};

      // Insert into our hash table
      table.TypedInsert(k.Hash(), k, v);

      // Insert into reference table
      ref_map.emplace(k, v);
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
        Value v = {.v1 = 0, .v2 = 1, .v3 = 2, .v4 = 3};
        table.TypedInsert(k.Hash(), k, v);
        num_keys++;
      }
    }

    EXPECT_EQ(num_keys, table.NumElements());

    uint32_t selection_vector[vec_size] = {0};
    codegen::util::HashTable::ScanState scan(table, selection_vector, vec_size);

    uint32_t entry_count = 0;
    while (!scan.Next()) {
      auto *entries = scan.Entries();
      for (uint32_t i = 0; i < scan.CurrentBatchSize(); i++) {
        const Key *k;
        const Value *v;
        entries[i]->GetKV(k, v);
        entry_count++;
      }
    }

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
      EXPECT_EQ(key.k1, v.v2)
          << "Key " << key << " inserted by thread " << key.k1
          << " but value was inserted by thread " << v.v2;
      count++;
    };
    global_table.TypedProbe(key.Hash(), key, f);
    EXPECT_EQ(1, count) << "Found duplicate keys in unique key test";
  }
}

TEST_F(HashTableTest, InsertPartitionedTest) {
  codegen::util::HashTable table(GetMemPool(), sizeof(Key), sizeof(Value));

  const auto num_groups = static_cast<uint32_t>(table.FlushThreshold() + 9);
  const auto to_insert = uint32_t(50000);

  std::vector<Key> keys;

  /// Generate test data
  for (uint32_t i = 0; i < to_insert; i++) {
    keys.emplace_back(i % num_groups, i % num_groups);
  }

  /// Insert partitioned
  for (uint32_t i = 0; i < to_insert; i++) {
    table.TypedUpsert<true, Key, Value>(keys[i].Hash(), keys[i],
                                        [](bool exists, Value *curr) {
                                          if (exists) {
                                            curr->v1++;
                                          } else {
                                            *curr = {1, 2, 3, 4};
                                          }
                                        });
  }

  // We should have flushed at least once
  EXPECT_TRUE(table.NumFlushes() > 0);

  // We can never have more elements than # groups
  EXPECT_FALSE(table.NumElements() > num_groups);
}

TEST_F(HashTableTest, ParallelPartitionedBuild) {
  constexpr uint32_t num_threads = 4;

  /*
   * This test mimics an aggregation by inserting upsert-ing elements into a
   * hash table. We vary the number of groups between 10 and 10,000, and each
   * aggregate is just a count(*) on the first element of the upsert-ed value
   * (i.e., Value.v1).
   *
   * We build 'num_threads' thread-local hash tables that each contain exactly
   * 'num_groups' groups, whose count will be 'scale_factor'. Thus, the global
   * aggregate total will be 'scale_factor' x 'num_threads'.
   *
   * In this test, we first construct thread-local hash tables with the correct
   * groups and aggregate counts. We then transfer thread-local table data to a
   * global table. We scan the global table, which merges the thread-local table
   * into a global view. We ensure the global aggregate counts match and ensure
   * we only see 'num_groups' unique grouping keys globally.
   */

  constexpr uint32_t scale_factor = 5;
  for (const uint32_t num_groups : {10u, 100u, 1000u, 10000u}) {
    executor::ExecutorContext ctx(nullptr);
    codegen::util::HashTable global_table(*ctx.GetPool(), sizeof(Key),
                                          sizeof(Value));

    // Setup thread state for "num_threads"
    ctx.GetThreadStates().Reset(sizeof(codegen::util::HashTable));
    ctx.GetThreadStates().Allocate(num_threads);

    //////////////////////////////////////////////////////////////////
    /// Do partitioned inserts into each thread-local table
    //////////////////////////////////////////////////////////////////
    {
      for (uint32_t tid = 0; tid < num_threads; tid++) {
        // First, initialize thread-local table
        auto &thread_states = ctx.GetThreadStates();
        auto *table =
            thread_states.AccessThreadStateAs<codegen::util::HashTable>(tid);
        codegen::util::HashTable::Init(*table, ctx, sizeof(Key), sizeof(Value));

        // Insert
        uint32_t num_keys = scale_factor * num_groups;
        for (uint32_t i = 0; i < num_keys; i++) {
          Key group_key(i % num_groups, i % num_groups);
          table->TypedUpsert<true, Key, Value>(group_key.Hash(), group_key,
                                               [](bool exists, Value *curr) {
                                                 if (exists) {
                                                   curr->v1++;
                                                 } else {
                                                   *curr = {1, 2, 3, 4};
                                                 }
                                               });
        }
      }
    }

    //////////////////////////////////////////////////////////////////
    /// Transfer partitions to global table
    //////////////////////////////////////////////////////////////////
    {
      // Given a linked-list overflow partition, build a single hash table
      auto partitioned_build = [](void *, codegen::util::HashTable &table,
                                  codegen::util::HashTable::Entry **partition,
                                  uint64_t begin_idx, uint64_t end_idx) {
        const Key *k;
        const Value *v;
        for (uint64_t idx = begin_idx; idx < end_idx; idx++) {
          auto *entry = partition[idx];
          while (entry != nullptr) {
            entry->GetKV(k, v);
            table.TypedUpsert<false, Key, Value>(entry->hash, *k,
                                                 [&v](bool found, Value *curr) {
                                                   if (found) {
                                                     curr->v1 += v->v1;
                                                   } else {
                                                     *curr = *v;
                                                   }
                                                 });
            entry = entry->next;
          }
        }
      };

      // The "0" is for the offset of the hash table in the state. It's 0
      // because it's the only element in the state and it's situated in the
      // first slot.
      global_table.TransferPartitions(ctx.GetThreadStates(), 0,
                                      partitioned_build);

      // Check
      auto &thread_states = ctx.GetThreadStates();
      for (uint32_t tid = 0; tid < num_threads; tid++) {
        auto *tl_table =
            thread_states.AccessThreadStateAs<codegen::util::HashTable>(tid);
        EXPECT_EQ(0, tl_table->NumElements());
      }

      // Reset
      thread_states.Reset(0);
    }

    //////////////////////////////////////////////////////////////////
    /// Final scan to check results
    //////////////////////////////////////////////////////////////////
    {
      class State {
       private:
        std::mutex mutex;
        uint32_t num_groups;

       public:
        const uint32_t agg_val;
        explicit State(uint32_t v) : num_groups(0), agg_val(v) {}
        void IncGroupCount() {
          std::lock_guard<std::mutex> lock(mutex);
          num_groups++;
        }
        uint32_t NumGroups() const { return num_groups; }
      };
      State state(scale_factor * num_threads);

      /*
       * Scan a given partition of the hash table. We just want to check that
       * the aggregate count is as expected, and increment the number of groups
       * we've seen.
       */
      auto scan_fn = [](void *query_state, void *,
                        const codegen::util::HashTable &table) {
        constexpr uint32_t vec_size = 512;
        // Pull out state
        auto *s = reinterpret_cast<State *>(query_state);
        // Start scan
        uint32_t vec[vec_size] = {0};
        codegen::util::HashTable::ScanState scan(table, vec, vec_size);
        while (!scan.Next()) {
          auto *entries = scan.Entries();
          for (uint32_t i = 0; i < scan.CurrentBatchSize(); i++) {
            const Key *k;
            const Value *v;
            entries[i]->GetKV(k, v);
            EXPECT_EQ(s->agg_val, v->v1);
            s->IncGroupCount();
          }
        }
      };

      // Execute scan over the global table
      codegen::util::HashTable::ExecutePartitionedScan(
          static_cast<void *>(&state), ctx.GetThreadStates(), global_table,
          scan_fn);

      EXPECT_EQ(num_groups, state.NumGroups());
    }
  }
}

}  // namespace test
}  // namespace peloton