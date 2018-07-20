//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table.h
//
// Identification: src/include/codegen/hash_table.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/compact_storage.h"
#include "codegen/value.h"
#include "codegen/vector.h"
#include "planner/attribute_info.h"

namespace peloton {

namespace planner {
struct AttributeInfo;
}  // namespace planner

namespace codegen {

/**
 * The main hash table access class for util::HashTable.
 */
class HashTable {
 public:
  /// A global pointer for attribute hashes
  static const planner::AttributeInfo kHashAI;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Forward declaration of helper classes
  ///
  //////////////////////////////////////////////////////////////////////////////

  class HashTableAccess;
  class ProbeCallback;
  class InsertCallback;
  class MergeCallback;
  class IterateCallback;
  class VectorizedIterateCallback;

  enum class InsertMode { Normal, Lazy, Partitioned };

 public:
  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Main HashTable interface
  ///
  //////////////////////////////////////////////////////////////////////////////

  HashTable();
  HashTable(CodeGen &codegen, const std::vector<type::Type> &key_type,
            uint32_t value_size);

  /**
   * Initialize the hash table instance.
   *
   * @param codegen The codegen instance
   * @param exec_ctx A pointer to the executor::ExecutorContext
   * @param ht_ptr A pointer to the hash table we'll initialize
   */
  void Init(CodeGen &codegen, llvm::Value *exec_ctx, llvm::Value *ht_ptr) const;

  /**
   * Probe the hash table and invoke the provided probe callback if a key match
   * is found. Otherwise, a new entry is inserted into the hash table with the
   * provided key and the insert callback is invoked to serialize the payload
   * data into the table.
   *
   * @param codegen The codegen instance
   * @param ht_ptr A pointer to the hash table we're modifying
   * @param hash The (optional) hash value for the key
   * @param key The key to probe with
   * @param insert_mode The mode, either regular or partitioned, to perform the
   * insertion
   * @param probe_callback The callback invoked when a key match is found
   * @param insert_callback The callback invoked when no key match exists
   */
  void ProbeOrInsert(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
                     const std::vector<codegen::Value> &key,
                     InsertMode insert_mode, ProbeCallback *probe_callback,
                     InsertCallback *insert_callback) const;

  /**
   * Perform a blind insert into the hash table.
   *
   * @param codegen The codegen instance
   * @param ht_ptr A pointer to the hash table we're inserting into
   * @param hash The (optional) hash value for the key
   * @param key The key to insert
   * @param mode The mode (regular, lazy or partitioned) to insert with
   * @param callback The callback invoked to serialize the value into the table
   */
  void Insert(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
              const std::vector<codegen::Value> &key, InsertMode mode,
              InsertCallback &callback) const;

  /**
   * Build a hash table over all tuple data materialized in the provided hash
   * table's value space.
   *
   * @param codegen The codegen instance
   * @param ht_ptr A pointer to the hash table we'll build lazily
   */
  void BuildLazy(CodeGen &codegen, llvm::Value *ht_ptr) const;

  /**
   * Reserve enough space in the provided hash table to store all elements
   * contained in each of the thread-local hash tables in the provided thread
   * states object. It is assumed that each of the thread-local tables were also
   * build lazily.
   *
   * @param codegen The codegen instance
   * @param ht_ptr A pointer to the main hash table we'll reserve/resize
   * @param thread_states A pointer to the thread states object where all thread
   * local hash tables are stored
   * @param ht_state_offset The offset in an individual thread's state
   * ThreadState object where the hash table instance exists.
   */
  void ReserveLazy(CodeGen &codegen, llvm::Value *ht_ptr,
                   llvm::Value *thread_states, uint32_t ht_state_offset) const;

  /**
   * Merge the contents of the given thread-local hash table into the provided
   * main global hash table.
   *
   * @param codegen The codegen instance
   * @param global_ht The global hash table that will accept new data
   * @param local_ht The thread-local table we'll read
   */
  void MergeLazyUnfinished(CodeGen &codegen, llvm::Value *global_ht,
                           llvm::Value *local_ht) const;

  /**
   * Merge all data stored in the partition range [begin_idx, end_idx] into the
   * provided target hash table. It is assumed that the partition list contains
   * hash table entries with the same key format as the table we're merging
   * into. The callback is invoked to merge the contents of an individual entry
   * in the overflow partition into an entry in the hash table. If no match is
   * found, a new entry is inserted into the hash table with the key and value
   * taken from the partition entry.
   *
   * @param codegen The codegen instance
   * @param target_ht_ptr A pointer to the hash table we'll merge the partition
   * data into
   * @param part_list A contiguous list of partition head pointers
   * @param begin_idx The start index of the partition we're merging
   * @param end_idx The end index of the partition we're merging
   * @param callback The callback invoked for each key match to merge contents
   * of the partition entry and the hash table entry
   */
  void MergePartitionRange(CodeGen &codegen, llvm::Value *target_ht_ptr,
                           llvm::Value *part_list, llvm::Value *begin_idx,
                           llvm::Value *end_idx, MergeCallback &callback) const;

  /**
   * Transfer all data stored in thread-local hash tables (in the provided
   * ThreadStates object) into the main hash table. Use the provided merging
   * function when ultimately coalescing an overflow partition.
   *
   * @param codegen The codegen instance
   * @param ht_ptr A pointer to the hash table into which all thread local data
   * is transfered to. This table will become the owner of all the data.
   * @param thread_states A pointer to the ThreadStates object
   * @param tl_ht_state_offset The offset in the ThreadStates object where each
   * thread-local hash table resides
   * @param merging_func The merging function to use when merging an overflow
   * partition into a hash table
   */
  void TransferPartitions(CodeGen &codegen, llvm::Value *ht_ptr,
                          llvm::Value *thread_states,
                          uint32_t tl_ht_state_offset,
                          llvm::Function *merging_func) const;

  /**
   *
   * @param codegen
   * @param ht_ptr
   * @param query_state
   */
  void FinishPartitions(CodeGen &codegen, llvm::Value *ht_ptr,
                        llvm::Value *query_state) const;

  /**
   *
   * @param codegen
   * @param ht_ptr
   * @param query_state
   */
  void Repartition(CodeGen &codegen, llvm::Value *ht_ptr) const;

  /**
   *
   * @param codegen
   * @param ht_ptr
   * @param query_state
   * @param merging_func
   */
  void MergePartitions(CodeGen &codegen, llvm::Value *ht_ptr,
                       llvm::Value *query_state, llvm::Value *target_ht_ptr,
                       llvm::Function *merging_func) const;

  /**
   *
   * @param codegen
   * @param dest_ht_ptr
   * @param src_ht_ptr
   * @param callback
   */
  void Merge(CodeGen &codegen, llvm::Value *dest_ht_ptr,
             llvm::Value *src_ht_ptr, MergeCallback &callback,
             bool multithreaded) const;

  /**
   * Iterate over all entries in the hash table, tuple-at-a-time.
   *
   * @param codegen The codegen instance
   * @param ht_ptr A pointer to the hash table we'll iterate over
   * @param callback The callback invoked for each entry in the table
   */
  void Iterate(CodeGen &codegen, llvm::Value *ht_ptr,
               IterateCallback &callback) const;

  /**
   * Iterate over all entries in the hash table using a vectorized technique.
   *
   * @param codegen The codegen instance
   * @param ht_ptr A pointer to the hash table we'll iterate over
   * @param selection_vector The selection vector used to reference entries
   * @param callback The callback invoked for each batch of table data
   */
  void VectorizedIterate(CodeGen &codegen, llvm::Value *ht_ptr,
                         Vector &selection_vector,
                         VectorizedIterateCallback &callback) const;

  /**
   * Find all entries that match the provided key, invoking the provided
   * callback for each match.
   *
   * @param codegen The codegen instance
   * @param ht_ptr A pointer to the hash table we're probing
   * @param key The key to probe
   * @param callback The callback invoked for each match
   */
  void FindAll(CodeGen &codegen, llvm::Value *ht_ptr,
               const std::vector<codegen::Value> &key,
               IterateCallback &callback) const;

  /**
   * Destroy the resources allocated by the provided hash table.
   *
   * @param codegen The codegen instance
   * @param ht_ptr A pointer to the hash table we're destroying
   */
  void Destroy(CodeGen &codegen, llvm::Value *ht_ptr) const;

  /**
   * Reads the key for the provided Entry, and returns a pointer to the payload
   *
   * @param codegen The codegen instance
   * @param entry_ptr A pointer to a HashTable::Entry
   * @param[out] key Output vector storing key components
   * @return A pointer to the payload within the Entry
   */
  llvm::Value *KeysForEntry(CodeGen &codegen, llvm::Value *entry_ptr,
                            std::vector<codegen::Value> &key) const;

 public:
  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Helper class definitions
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * This callback functor is used when probing the hash table for a given
   * key. It is invoked when a matching key-value pair is found in the hash
   * table.
   */
  class ProbeCallback {
   public:
    /** Virtual destructor */
    virtual ~ProbeCallback() = default;

    /**
     * The primary callback method. This is invoked for each matching key-value
     * pair. The value parameter is a pointer into the entry to an opaque set of
     * bytes that are the value associated to the input key. It is up to the
     * caller to interpret the bytes.
     *
     * @param codegen The code generator instance
     * @param value A pointer to the value bytes
     */
    virtual void ProcessEntry(CodeGen &codegen, llvm::Value *value) const = 0;
  };

  /**
   * A callback used when inserting a new entry into the hash table. The caller
   * implements StoreValue() to perform the insertion, and GetValueSize() to
   * indicate the number of bytes needed to store the value associated with the
   * inserted key.
   */
  class InsertCallback {
   public:
    /** Virtual destructor */
    virtual ~InsertCallback() = default;

    /**
     * Serialize the value into a provided memory space in the hash table.
     *
     * @param codegen The codegen instance
     * @param space The memory space for the value
     */
    virtual void StoreValue(CodeGen &codegen, llvm::Value *space) const = 0;

    /**
     * Return the number of bytes for the value
     *
     * @param codegen The codegen instance
     * @return The number of bytes needed to store the value
     */
    virtual llvm::Value *GetValueSize(CodeGen &codegen) const = 0;
  };

  /**
   *
   */
  class MergeCallback {
   public:
    virtual ~MergeCallback() = default;

    virtual void MergeValues(CodeGen &codegen, llvm::Value *table_values,
                             llvm::Value *new_values) const = 0;
  };

  /**
   * A callback used when iterating over the entries in the hash table.
   * ProcessEntry() is invoked for each entry in the table, or only those
   * entries that match a provided key if a search key is provided.
   */
  class IterateCallback {
   public:
    /** Virtual destructor */
    virtual ~IterateCallback() = default;

    /**
     * The primary callback function for each entry in the table, or for each
     * matching key-value pair when provided a search key.
     *
     * @param codegen The codegen instance
     * @param key The key stored in the hash table
     * @param values A pointer to a set of bytes where the value is stored
     */
    virtual void ProcessEntry(CodeGen &codegen, llvm::Value *entry_ptr,
                              const std::vector<codegen::Value> &key,
                              llvm::Value *values) const = 0;
  };

  /**
   * A callback used when performing a batched/vectorized iteration over the
   * entries in the hash table. Iteration may be over the entire table, or a
   * subset of the table if a matching probing key was provided.
   */
  class VectorizedIterateCallback {
   public:
    /** Virtual destructor */
    virtual ~VectorizedIterateCallback() = default;

    /**
     * Process a vector of entries in the hash table.
     *
     * @param codegen The codegen instance
     * @param start
     * @param end
     * @param selection_vector A vector containing indexes of valid entries
     * @param access A hash-table random-access helper
     */
    virtual void ProcessEntries(CodeGen &codegen, llvm::Value *start,
                                llvm::Value *end, Vector &selection_vector,
                                HashTableAccess &access) const = 0;
  };

  /**
   * Convenience class proving a random access interface over the hash-table
   */
  class HashTableAccess {
   public:
    /** Virtual destructor */
    virtual ~HashTableAccess() = default;

    /**
     * Extracts the key of an entry at a given index into the hash table storing
     * results into the output 'keys' vector.
     *
     * @param codegen The codegen instance
     * @param index The index in the directory
     * @param[out] keys Where each column of the key is stored
     */
    virtual void ExtractBucketKeys(CodeGen &codegen, llvm::Value *index,
                                   std::vector<codegen::Value> &keys) const = 0;

    /**
     * Returns a pointer to a value stored at the entry at the given index.
     *
     * @param codegen The codegen instance
     * @param index An index in the directory
     * @return A pointer to where the value is serialized
     */
    virtual llvm::Value *BucketValue(CodeGen &codegen,
                                     llvm::Value *index) const = 0;
  };

 private:
  // The size of the payload value store in the hash table
  uint32_t value_size_;

  // The storage strategy we use to store the lookup keys inside every HashEntry
  CompactStorage key_storage_;
};

}  // namespace codegen
}  // namespace peloton