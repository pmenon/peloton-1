//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table.h
//
// Identification: src/include/codegen/util/hash_table.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "libcount/hll.h"

#include "executor/executor_context.h"

namespace peloton {

namespace type {
class AbstractPool;
}  // namespace type

namespace codegen {
namespace util {

/**
 * This is a bucket-chained hash table that separates value storage from the
 * primary hash table directory (a la VectorWise). The hash table supports two
 * three modes: normal, partitioned, and lazy.
 *
 * Normal mode:
 * ------------
 * In normal mode, inserts and probes can be interleaved. Calls to Insert()
 * allocate storage space and acquire a slot in the hash table directory,
 * resizing by doubling if appropriate.
 *
 * Partitioned mode:
 * -----------------
 * Partitioned mode is designed for large-scale aggregations. In this mode, no
 * duplicate keys are allowed.
 *
 * Lazy mode:
 * ----------
 * In lazy mode, the hash table becomes a two-phase write-once-read-many (WORM)
 * structure. In the first phase, only inserts are allowed. After all inserts
 * are complete, the table "frozen" after which the table becomes read-only.
 *
 * In lazy mode, InsertLazy(), BuildLazy(), ReserveLazy(), and
 * MergeLazyUnfinished() should be used. InsertLazy() only acquires storage
 * space, BuildLazy() is called to freeze the hash table. ReserveLazy() is
 * called during parallel build to collect sizing information from all
 * thread-local hash tables to. Finally, calls to MergeLazyUnfinished() are
 * made concurrently from multiple threads to merge lazily-built thread-local
 * hash tables.
 */
class HashTable {
 public:
  // Forward declare
  struct Entry;

  /**
   * Primary constructor.
   *
   * @param memory The memory pool used for all memory allocations
   * @param key_size The size (in bytes) of
   * @param value_size The size (in bytes) of values
   */
  HashTable(::peloton::type::AbstractPool &memory, uint32_t key_size,
            uint32_t value_size);

  /**
   * Primary destructor.
   */
  ~HashTable();

  /**
   * Initialize the provided hash table. This is a static method used by codegen
   * to initialize a pre-allocated hash table instance.
   *
   * @param table The table we're setting up
   * @param key_size The size of the keys in bytes
   * @param value_size The size of the values in bytes
   */
  static void Init(HashTable &table, executor::ExecutorContext &exec_ctx,
                   uint32_t key_size, uint32_t value_size);

  /**
   * Clean up all resources allocated by the provided table. This is a static
   * method used by codegen to invoke the destructor of the hash table.
   *
   * @param table The table we're cleaning up
   */
  static void Destroy(HashTable &table);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Normal mode
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Insert a new key-value pair whose key has the provided hash value. This
   * method returns a pointer to a memory space where the key and value can be
   * serialized contiguously. Post insertion, the inserted key-value pair is
   * immediately visible to subsequent probes into the hash table.
   *
   * This generic version assumes opaque key and value bytes.
   * See HashTable::TypedInsert() for a strongly typed insertion method.
   *
   * @param hash The hash value of the key in the key-value pair to be inserted
   *
   * @return A (contiguous) memory region where the key and value can be stored
   */
  char *Insert(uint64_t hash);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Partitioned mode
  ///
  //////////////////////////////////////////////////////////////////////////////

  using MergingFunction = void (*)(void *query_state, HashTable &table,
                                   HashTable::Entry **partition,
                                   uint64_t begin_idx, uint64_t end_idx);

  using MergeEntryFunction = void (*)(void *query_state,
                                      HashTable::Entry *entry,
                                      HashTable &table);

  using ScanFunction = void (*)(void *query_state, void *thread_state,
                                const HashTable &table);

  /**
   * Insert a new key-value pair whose key has the provided hash value. This
   * method, like HashTable::Insert(), returns a pointer to a memory space where
   * the key and value can be serialized contiguously. The inserted key-value
   * pair may or may not be visible after this call. However, all key-value
   * pairs will be visible after HashTable::BuildPartitioned().
   *
   * @param hash The hash value of the key in the key-value pair to be inserted
   *
   * @return A (contiguous) memory region where the key and value can be stored
   */
  char *InsertPartitioned(uint64_t hash);

  /**
   * Transfer all overflow partitions stored in each thread-local hash table
   * (within ExecutorContext::ThreadStates) into this hash table. No hash table
   * construction is done here.
   *
   * @param thread_states The container holding all thread states
   * @param ht_offset The offset in each thread-local state where the
   * thread-local hash table resides.
   * @param merge_func The function used to build a partitioned hash table by
   * merging the contents of a set of overflow partitions
   */
  void TransferPartitions(
      const executor::ExecutorContext::ThreadStates &thread_states,
      uint32_t ht_offset, MergingFunction merge_func);

  /**
   * Execute a parallel scan over this partitioned hash table. The thread states
   * object has been configured with the appropriate size (from codegen'd code)
   * prior to this call. This function will configure the number of scan threads
   * based on the size of the table.
   *
   * The callback scan function accepts two opaque state objects: an query state
   * and a thread state. The query state is provided as a function argument. The
   * thread state will be pulled from the provided ThreadStates object.
   *
   * @param query_state The (opaque) query state
   * @param thread_states The container holding all thread states
   * @param table The hash table we'll scan over
   * @param scan_func The callback scan function that will scan one partition of
   * the partitioned hash table
   */
  static void ExecutePartitionedScan(
      void *query_state, executor::ExecutorContext::ThreadStates &thread_states,
      HashTable &table, ScanFunction scan_func);

  /**
   *
   * @param query_state
   */
  void FinishPartitions(void *query_state);

  /**
   * Repartition all data stored in this partitioned hash table
   */
  void Repartition();

  /**
   * Merge the overflow partitions stored in this hash table into the build
   * partitioned hash tables stored in the target hash table. We assume that
   * the target hash table is partitioned.
   *
   * @param query_state An opaque state object pointer
   * @param target The target hash table we merge our overflow partitions into
   * @param merge_func The function we use to merge a single entry in our hash
   * table into the target table.
   */
  void MergePartitions(void *query_state, HashTable &target,
                       MergeEntryFunction merge_func);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Lazy mode
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Insert a new key-value pair whose key has the provided hash value. This
   * method, like HashTable::Insert(), returns a pointer to a memory space where
   * the key and value can be serialized contiguously. Unlike Insert(), the
   * inserted key-value pair is not immediately visible after the insertion
   * completes. It will only become visible after a call to BuildLazy() or
   * MergeLazyUnfinished() (if in parallel mode).
   *
   * This generic version assumes opaque key and value bytes.
   * See HashTable::TypedInsertLazy() for a strongly typed insertion method.
   *
   * @param hash The hash value of the key in the key-value pair to be inserted
   *
   * @return A (contiguous) memory region where the key and value can be stored
   */
  char *InsertLazy(uint64_t hash);

  /**
   * This function builds a hash table over all elements that have been lazily
   * inserted into the hash table. It is assumed that this hash table is being
   * used in two-phase mode, i.e., where all insertions are performed first,
   * followed by a series of probes.
   *
   * The hash table transitions to read-only after this call.
   */
  void BuildLazy();

  /**
   * This function inspects the size of each thread-local hash table stored in
   * the thread states argument to perfectly size a 50% loaded hash table.
   *
   * This function is called during parallel hash table builds once each thread
   * has finished constructing its own thread-local hash table. The final phase
   * is to build a global hash table (this one) with pointers into thread-local
   * hash tables.
   *
   * @param thread_states Where thread-local hash tables are located
   * @param hash_table_offset The offset into each state where the thread-local
   * hash table can be found.
   */
  void ReserveLazy(const executor::ExecutorContext::ThreadStates &thread_states,
                   uint32_t hash_table_offset);

  /**
   * This function merges the contents of the provided hash table into this
   * hash table. The provided hash table is assumed to have been built lazily,
   * and has not finished construction of the directory; that is, each table has
   * only buffered tuple data into its storage area, but not into the hash table
   * directory.
   *
   * This function assumes a prior call to @refitem ReserveLazy() to allocate
   * sufficient space for all hash tables that will be merged. Hence, this
   * function does not allocate any memory.
   *
   * NOTE: This function is called from different threads!
   *
   * @param The hash table whose contents we will merge into this one.
   */
  void MergeLazyUnfinished(HashTable &other);

  /**
   * Resize the hash table
   */
  void Grow();

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Scanning
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Scanner iterator interface. Mostly used from codegen.
   */
  class ScanState {
   public:
    explicit ScanState(const HashTable &table, uint32_t *sel,
                       uint32_t sel_size);

    ~ScanState();

    /**
     * Initialize the provide scan state. This is a static method used from
     * codegen to initialize a pre-allocated scan state instance.
     *
     * @param scan_state The scan state we're initializing
     * @param table The hash table we're scanning over
     * @param sel A selection vector determining which (subset) of elements are
     * valid in the entries view.
     * @param sel_size The size of the selection vector
     *
     * @return True if there is data to scan. False otherwise.
     */
    static void Init(ScanState &scan_state, const HashTable &table,
                     uint32_t *sel, uint32_t sel_size);

    /**
     * Clean up all resources allocated by the provided scan state. This is a
     * static method used from codegen to invoke the destructor of the scanner.
     *
     * @param scan_state The scanner we wish to clean up.
     */
    static void Destroy(ScanState &scan_state);

    /**
     * Move the scanner to the next batch of valid entries in the hash table.
     *
     * @return True if there is data to scan. False otherwise.
     */
    bool Next();

    /**
     * Return the current batch of entries
     *
     * @return
     */
    const Entry *const *Entries() const { return entries_; }

    /**
     *
     * @return
     */
    uint32_t CurrentBatchSize() const { return size_; }

   private:
    // The hash table we're scanning
    const HashTable &table_;

    // The selection vector
    uint32_t *sel_;

    // The entries view where to temporarily cache valid hash table entries
    Entry **entries_;

    // The next index from the hash table directory to read
    uint64_t index_;

    // The next position in the active bucket we're reading from
    Entry *next_;

    // The number of valid elements in the scanner
    uint32_t size_;

    // The size of the selection vector
    uint32_t sel_size_;

    // A boolean indicating if there is more data to read
    bool done_;
  };

  friend class ScanState;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  uint64_t NumElements() const { return num_elems_; }

  uint64_t Capacity() const { return capacity_; }

  double LoadFactor() const { return num_elems_ / 1.0 / directory_size_; }

  uint64_t FlushThreshold() const { return flush_threshold_; }

  uint64_t NumFlushes() const { return stats_.num_flushes; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Testing Utilities
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Insert a key-value pair into the hash table. This function is used mostly
   * for testing since we specialize insertions in generated code.
   *
   * @param hash The hash value of the key
   * @param key The key to store in the table
   * @param value The value to store in the value
   */
  template <typename Key, typename Value>
  void TypedInsert(uint64_t hash, const Key &key, const Value &value);

  /**
   * Insert a key-value pair lazily into the hash table. This function is used
   * mostly for testing since we specialize insertions in generated code.
   *
   * @param hash The hash value of the key
   * @param key The key to store in the table
   * @param value The value to store in the value
   */
  template <typename Key, typename Value>
  void TypedInsertLazy(uint64_t hash, const Key &key, const Value &value);

  /**
   *
   * @tparam Partitioned
   * @tparam Key
   * @tparam Value
   * @param hash
   * @param key
   * @param update_func
   */
  template <bool Partitioned, typename Key, typename Value>
  void TypedUpsert(uint64_t hash, const Key &key,
                   const std::function<void(bool, Value *)> &update_func);

  /**
   * Probe a key in the hash table. This function is used mostly for testing.
   * Probing is completely specialized in generated code.
   *
   * @param hash The hash value of the key
   * @param key The key to probe in the table
   * @param[out] value The value associated with the key in the table
   * @return True if a value was found. False otherwise.
   */
  template <typename Key, typename Value>
  bool TypedProbe(uint64_t hash, const Key &key,
                  const std::function<void(const Value &)> &consumer_func);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Helper Classes
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * An entry in the hash table that stores a key and value. Entries are
   * variable-sized structs. In memory, the key and value bytes are stored
   * contiguously after the next pointer. The layout is thus:
   *
   * +--------------+---------------+-------------------+--------------------+
   * |  hash value  | next pointer  | sizeof(Key) bytes | sizeof(Val) bytes) |
   * +--------------+---------------+-------------------+--------------------+
   */
  struct Entry {
    uint64_t hash;
    Entry *next;
    char data[0];

    Entry(uint64_t _hash, Entry *_next) : hash(_hash), next(_next) {}

    static uint32_t Size(uint32_t key_size, uint32_t value_size) {
      return sizeof(Entry) + key_size + value_size;
    }

    template <typename Key, typename Value>
    void GetKV(const Key *&k, const Value *&v) const {
      k = reinterpret_cast<const Key *>(data);
      v = reinterpret_cast<const Value *>(data + sizeof(Key));
    };
  };

  /**
   * Memory blocks are contiguous chunks of memory used to store key-value
   * payloads in the hash table. Value storage is separated from the main
   * directory.
   *
   * Memory blocks reserve the first 8-bytes for a pointer to another memory
   * block, thus forming a linked list of blocks.
   */
  struct MemoryBlock {
    MemoryBlock *next;
    char data[0];
  };

  /**
   * A structure to capture various operational statistics over the lifetime use
   * of this hash table.
   */
  struct Stats {
    uint64_t num_grows = 0;
    uint64_t num_flushes = 0;
  };

 private:
  /**
   * Determines if this hash table has to be resized.
   *
   * @return True if the hash table should grow
   */
  bool NeedsToGrow() const { return num_elems_ == capacity_; }

  /**
   * Allocate a new Entry object from value storage for a new key-value pair.
   *
   * @param hash The hash value of the new key
   *
   * @return An Entry where the key-value pair can be stored
   */
  Entry *NewEntry(uint64_t hash);

  /**
   * Transfer all allocated memory blocks in this hash table to the provided
   * target table. This needs to be performed in a thread-safe manner since
   * the target can be under concurrent modification.
   *
   * @param target The hash table that takes ownership of our allocated memory
   */
  void TransferMemoryBlocks(HashTable &target);

  /**
   * Flush and redistribute all data stored in the hash table into the set of
   * overflow partitions. This completely clears the hash table (i.e., the hash
   * table will indicate that no elements are stored).
   */
  void FlushToOverflowPartitions();

  /**
   * Build a new hash table from the tuples stored in the partition with the
   * provided ID. This newly constructed hash table is owned by this one.
   *
   * @param partition_id The ID of the partition we'll create the hash table on
   *
   * @return The constructed partition. This should be the same as in
   * part_tables_[partition_id]
   */
  HashTable *BuildPartitionedTable(void *query_state, uint32_t partition_id);

  /**
   * Reserve room in this hash table for at least the provided number of
   * elements, targeting a 50% factor.
   *
   * @param num_elems The number of elements to size the table for
   */
  void Reserve(uint32_t num_elems);

 private:
  // The memory allocator used for all allocations in this hash table
  ::peloton::type::AbstractPool &memory_;
  uint32_t key_size_;
  uint32_t value_size_;

  // The directory of the hash table
  Entry **directory_;
  uint64_t directory_size_;
  uint64_t directory_mask_;

  // The current active block
  MemoryBlock *block_;

  // A pointer into the active memory block where the next free entry is
  char *next_entry_;

  // The number of available bytes left in the block
  uint64_t available_bytes_;

  // The number of elements stored in this hash table, and the max before it
  // needs to be resized
  uint64_t num_elems_;
  uint64_t capacity_;

  // Overflow partitions
  MergingFunction merging_func_;
  Entry **part_heads_;
  Entry **part_tails_;
  HashTable **part_tables_;
  uint64_t flush_threshold_;
  uint64_t part_shift_bits_;

  // Stats
  Stats stats_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

template <typename Key, typename Value>
void HashTable::TypedInsert(uint64_t hash, const Key &key, const Value &value) {
  auto *data = Insert(hash);
  *reinterpret_cast<Key *>(data) = key;
  *reinterpret_cast<Value *>(data + sizeof(Key)) = value;
}

template <typename Key, typename Value>
void HashTable::TypedInsertLazy(uint64_t hash, const Key &key,
                                const Value &value) {
  auto *data = InsertLazy(hash);
  *reinterpret_cast<Key *>(data) = key;
  *reinterpret_cast<Value *>(data + sizeof(Key)) = value;
}

template <bool Partitioned, typename Key, typename Value>
void HashTable::TypedUpsert(
    uint64_t hash, const Key &key,
    const std::function<void(bool, Value *)> &update_func) {
  // Lookup
  auto *entry = directory_[hash & directory_mask_];

  while (entry != nullptr) {
    if (entry->hash == hash && *reinterpret_cast<Key *>(entry->data) == key) {
      update_func(true, reinterpret_cast<Value *>(entry->data + sizeof(Key)));
      return;
    }
    entry = entry->next;
  }

  // Key doesn't exist in table, insert now
  char *ret = (Partitioned ? InsertPartitioned(hash) : Insert(hash));
  *reinterpret_cast<Key *>(ret) = key;
  update_func(false, reinterpret_cast<Value *>(ret + sizeof(Key)));
}

template <typename Key, typename Value>
bool HashTable::TypedProbe(
    uint64_t hash, const Key &key,
    const std::function<void(const Value &)> &consumer_func) {
  // Initial index in the directory
  uint64_t index = hash & directory_mask_;

  auto *entry = directory_[index];
  if (entry == nullptr) {
    return false;
  }

  bool found = false;
  while (entry != nullptr) {
    if (entry->hash == hash && *reinterpret_cast<Key *>(entry->data) == key) {
      auto *value = reinterpret_cast<Value *>(entry->data + sizeof(Key));
      consumer_func(*value);
      found = true;
    }
    entry = entry->next;
  }

  // Not found
  return found;
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton