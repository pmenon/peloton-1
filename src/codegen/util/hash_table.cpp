//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table.cpp
//
// Identification: src/codegen/util/hash_table.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/hash_table.h"

#include <numeric>

#include "common/platform.h"
#include "common/synchronization/count_down_latch.h"
#include "threadpool/mono_queue_pool.h"
#include "type/abstract_pool.h"

namespace peloton {
namespace codegen {
namespace util {

static constexpr uint32_t kDefaultNumElements = 128;
static constexpr uint32_t kDefaultNumPartitions = 512;
static constexpr uint32_t kNumBlockElems = 1024;

static_assert((kDefaultNumElements & (kDefaultNumElements - 1)) == 0,
              "Default number of elements must be a power of two");

HashTable::HashTable(::peloton::type::AbstractPool &memory, uint32_t key_size,
                     uint32_t value_size)
    : memory_(memory),
      key_size_(key_size),
      value_size_(value_size),
      directory_(nullptr),
      directory_size_(0),
      directory_mask_(0),
      block_(nullptr),
      next_entry_(nullptr),
      available_bytes_(0),
      num_elems_(0),
      capacity_(kDefaultNumElements),
      merging_func_(nullptr),
      part_heads_(nullptr),
      part_tails_(nullptr),
      part_tables_(nullptr),
      flush_threshold_(capacity_) {
  // Upon creation, we allocate room for kDefaultNumElements in the hash table.
  // We assume 50% load factor on the directory, thus the directory size is
  // twice the number of elements.
  directory_size_ = capacity_ * 2;
  directory_mask_ = directory_size_ - 1;

  uint64_t alloc_size = sizeof(Entry *) * directory_size_;
  directory_ = static_cast<Entry **>(memory_.Allocate(alloc_size));

  PELOTON_MEMSET(directory_, 0, alloc_size);

  // We also need to allocate some space to store tuples. Tuples are stored
  // externally from the main hash table in a separate values memory space.
  auto entry_size = key_size + value_size;
  uint64_t block_size = sizeof(MemoryBlock) + (entry_size * kNumBlockElems);
  block_ = reinterpret_cast<MemoryBlock *>(memory_.Allocate(block_size));
  block_->next = nullptr;

  // Set the next tuple write position and the available bytes
  next_entry_ = block_->data;
  available_bytes_ = block_size - sizeof(MemoryBlock);
}

HashTable::~HashTable() {
  // Free memory blocks
  if (block_ != nullptr) {
    // Free all the blocks we've allocated
    MemoryBlock *block = block_;
    while (block != nullptr) {
      MemoryBlock *next = block->next;
      memory_.Free(block);
      block = next;
    }
    block_ = nullptr;
  }

  // Free the directory
  if (directory_ != nullptr) {
    memory_.Free(directory_);
    directory_ = nullptr;
  }

  // Free partitions
  if (part_heads_ != nullptr) {
    memory_.Free(part_heads_);
    part_heads_ = nullptr;
  }

  if (part_tails_ != nullptr) {
    memory_.Free(part_tails_);
    part_tails_ = nullptr;
  }

  if (part_tables_ != nullptr) {
    for (uint32_t i = 0; i < kDefaultNumPartitions; i++) {
      if (part_tables_[i] != nullptr) {
        HashTable::Destroy(*part_tables_[i]);
        memory_.Free(part_tables_[i]);
      }
    }
    memory_.Free(part_tables_);
    part_tables_ = nullptr;
  }
}

void HashTable::Init(HashTable &table, executor::ExecutorContext &exec_ctx,
                     uint32_t key_size, uint32_t value_size) {
  new (&table) HashTable(*exec_ctx.GetPool(), key_size, value_size);
}

void HashTable::Destroy(HashTable &table) { table.~HashTable(); }

char *HashTable::Insert(uint64_t hash) {
  // Resize the hash table if needed
  if (NeedsToGrow()) {
    Grow();
  }

  // Allocate an entry from storage
  Entry *entry = NewEntry(hash);

  // Insert into hash table
  uint64_t index = hash & directory_mask_;
  entry->next = directory_[index];
  directory_[index] = entry;

  // Bump stats
  num_elems_++;

  // Return data pointer for key/value storage
  return entry->data;
}

char *HashTable::InsertPartitioned(uint64_t hash) {
  char *ret = Insert(hash);
  if (num_elems_ >= flush_threshold_) {
    FlushToOverflowPartitions();
  }
  return ret;
}

void HashTable::FlushToOverflowPartitions() {
  if (part_heads_ == nullptr) {
    PELOTON_ASSERT(part_tails_ == nullptr);
    size_t num_bytes = sizeof(Entry *) * kDefaultNumPartitions;
    part_heads_ = static_cast<Entry **>(memory_.Allocate(num_bytes));
    part_tails_ = static_cast<Entry **>(memory_.Allocate(num_bytes));
    PELOTON_MEMSET(part_heads_, 0, num_bytes);
    PELOTON_MEMSET(part_tails_, 0, num_bytes);
  }

  for (uint64_t i = 0; i < directory_size_; i++) {
    Entry *entry = directory_[i];

    // Move whole bucket chain to overflow partitions
    while (entry != nullptr) {
      // Grab a handle to the next entry
      Entry *next = entry->next;

      // Move entry into appropriate partition
      // TODO: partition ID computation
      uint64_t part_idx = (entry->hash >> 8ul) & 0x1ff;
      entry->next = part_heads_[part_idx];
      part_heads_[part_idx] = entry;
      if (part_tails_[part_idx] == nullptr) {
        part_tails_[part_idx] = entry;
      }

      // Move along
      entry = next;
    }

    // Empty the directory slot
    directory_[i] = nullptr;
  }

  // No elements in the directory
  num_elems_ = 0;

  // Increment number of flushes
  stats_.num_flushes++;
}

const HashTable *HashTable::BuildPartitionedTable(void *query_state,
                                                  uint32_t partition_id) {
  // Sanity checks
  PELOTON_ASSERT(part_heads_ != nullptr &&
                 "Partition heads array not allocated");
  PELOTON_ASSERT(part_heads_[partition_id] != nullptr &&
                 "No head for the overflow partition exists");
  PELOTON_ASSERT(part_tables_[partition_id] == nullptr &&
                 "No table for the overflow partition exists");
  PELOTON_ASSERT(merging_func_ != nullptr &&
                 "No merging function is set. You should only call this "
                 "through BuildPartitioned() or TransferPartitions() ...");

  // Allocate a table
  auto *table = new (memory_.Allocate(sizeof(HashTable)))
      HashTable(memory_, key_size_, value_size_);

// Reserve based on partition sample
#if 0
  table->Reserve(
      static_cast<uint32_t>(part_samples_[partition_id]->Estimate()));
#endif

  // Assign into partition
  part_tables_[partition_id] = table;

  // Merge the partition into the table using the compiled merging function
  merging_func_(query_state, *table, part_heads_, partition_id,
                partition_id + 1);

  // Done
  return table;
}

void HashTable::TransferPartitions(
    const executor::ExecutorContext::ThreadStates &thread_states,
    uint32_t ht_offset) {
  // Collect all thread-local hash tables
  std::vector<HashTable *> tables;
  thread_states.ForEach<HashTable>(ht_offset, [&tables](HashTable *hash_table) {
    tables.emplace_back(hash_table);
  });

  // Flush each thread-local hash table
  for (auto *table : tables) {
    table->FlushToOverflowPartitions();
  }

  // Transfer overflow partitions from each thread-local hash table to us
  for (auto *table : tables) {
    for (uint32_t part_idx = 0; part_idx < kDefaultNumPartitions; part_idx++) {
      if (table->part_heads_[part_idx] != nullptr) {
        // If there's a head pointer, there must be a tail pointer too
        PELOTON_ASSERT(table->part_tails_[part_idx] != nullptr);
        table->part_tails_[part_idx]->next = part_heads_[part_idx];
        part_heads_[part_idx] = table->part_heads_[part_idx];
        if (part_tails_[part_idx] == nullptr) {
          part_tails_[part_idx] = table->part_tails_[part_idx];
        }
      }
    }
  }
}

void HashTable::ExecutePartitionedScan(
    void *query_state, executor::ExecutorContext::ThreadStates &thread_states,
    MergingFunction merge_func, PartitionedScanFunction scan_func) {
  // Set the merging function
  PELOTON_ASSERT(merge_func != nullptr);
  merging_func_ = merge_func;

  // Allocate partition tables array
  PELOTON_ASSERT(part_tables_ == nullptr);
  {
    auto num_bytes = sizeof(HashTable) * kDefaultNumPartitions;
    part_tables_ = static_cast<HashTable **>(memory_.Allocate(num_bytes));
    PELOTON_MEMSET(part_tables_, 0, num_bytes);
  }

  // Count number of partitions to build
  uint32_t num_parts = 0;
  for (uint32_t part_idx = 0; part_idx < kDefaultNumPartitions; part_idx++) {
    num_parts++;
  }

  // Distribute work
  {
    // Worker pool
    auto &worker_pool = threadpool::MonoQueuePool::GetExecutionInstance();

    // Allocate a slot for each worker thread
    thread_states.Allocate(worker_pool.NumWorkers());

    // The latch
    common::synchronization::CountDownLatch latch(num_parts);

    // Iterate over all partitions, spawning work to build tables
    for (uint32_t part_idx = 0; part_idx < kDefaultNumPartitions; part_idx++) {
      if (part_heads_[part_idx] != nullptr) {
        worker_pool.SubmitTask([this, &query_state, &thread_states, scan_func,
                                &latch, part_idx]() {
          // Build partitioned table
          auto *table = BuildPartitionedTable(query_state, part_idx);

          // Send to scan function
          // TODO: thread states
          scan_func(query_state, thread_states.AccessThreadState(0), *table);

          // Count down
          latch.CountDown();
        });
      }
    }

    // Wait
    latch.Await(0);
  }
}

char *HashTable::InsertLazy(uint64_t hash) {
  // Since this is a lazy insertion, we only allocate an entry. We don't insert
  // the entry into its correct position in the hash table until a call to
  // BuildLazy() or MergeLazy() is made.
  auto *entry = NewEntry(hash);

  // Insert the entry into the linked list in the first directory slot
  if (directory_[0] == nullptr) {
    // This is the first entry. The first slot in the directory is the head of
    // the linked list of entries. All other's link their tail to the second
    // element in the directory.
    directory_[0] = entry;
    directory_[1] = entry;
  } else {
    PELOTON_ASSERT(directory_[1] != nullptr);
    directory_[1]->next = entry;
    directory_[1] = entry;
  }

  num_elems_++;

  // Return data pointer for key/value storage
  return entry->data;
}

void HashTable::BuildLazy() {
  // Early exit if no elements have been added (hash table is still valid)
  if (num_elems_ == 0) return;

  // Grab entry head
  Entry *head = directory_[0];

  // Clean up old directory
  memory_.Free(directory_);

  // At this point, all the lazy insertions are assumed to have completed. We
  // can allocate a perfectly sized hash table with 50% load factor.
  //
  // TODO: Use sketches to estimate the real # of unique elements
  // TODO: Perhaps change probing strategy based on estimate?

  directory_size_ = NextPowerOf2(num_elems_) * 2;
  directory_mask_ = directory_size_ - 1;

  uint64_t alloc_size = sizeof(Entry *) * directory_size_;
  directory_ = static_cast<Entry **>(memory_.Allocate(alloc_size));

  PELOTON_MEMSET(directory_, 0, alloc_size);

  // Now insert all elements into the directory
  while (head != nullptr) {
    uint64_t index = head->hash & directory_mask_;
    Entry *next = head->next;
    head->next = directory_[index];
    directory_[index] = head;
    head = next;
  }
}

void HashTable::ReserveLazy(
    const executor::ExecutorContext::ThreadStates &thread_states,
    uint32_t hash_table_offset) {
  // Determine the total number of tuples stored across each hash table
  uint64_t total_size = 0;
  for (uint32_t i = 0; i < thread_states.NumThreads(); i++) {
    auto *hash_table = reinterpret_cast<HashTable *>(
        thread_states.AccessThreadState(i) + hash_table_offset);
    total_size += hash_table->NumElements();
  }

  // TODO: Combine sketches to estimate the true unique # of elements

  // Perfectly size the hash table
  num_elems_ = 0;
  capacity_ = NextPowerOf2(total_size);

  directory_size_ = capacity_ * 2;
  directory_mask_ = directory_size_ - 1;

  uint64_t alloc_size = sizeof(Entry *) * directory_size_;
  directory_ = static_cast<Entry **>(memory_.Allocate(alloc_size));
  PELOTON_MEMSET(directory_, 0, alloc_size);
}

void HashTable::MergeLazyUnfinished(HashTable &other) {
  // Begin with the head of the linked list of entries, stored in the first
  // directory entry
  auto *head = other.directory_[0];

  while (head != nullptr) {
    // Compute the index and stash the next entry in the linked list
    uint64_t index = head->hash & directory_mask_;
    Entry *next = head->next;

    // Try to CAS in this entry into the directory
    Entry *curr;
    do {
      curr = directory_[index];
      head->next = curr;
    } while (!::peloton::atomic_cas(directory_ + index, curr, head));

    // Success, move along
    head = next;
  }

  // Increment number of elements
  ::peloton::atomic_add(&num_elems_, other.NumElements());

  // Transfer all allocated memory blocks in the other table into this one
  PELOTON_ASSERT(&memory_ == &other.memory_);
  other.TransferMemoryBlocks(*this);
}

void HashTable::Grow() {
  // Double the capacity
  capacity_ *= 2;

  // Allocate the new directory with 50% fill factor
  uint64_t new_dir_size = capacity_ * 2;
  uint64_t new_dir_mask = new_dir_size - 1;

  uint64_t alloc_size = sizeof(Entry *) * new_dir_size;
  auto *new_dir = static_cast<Entry **>(memory_.Allocate(alloc_size));
  PELOTON_MEMSET(new_dir, 0, alloc_size);

  // Insert all old directory entries into new directory
  for (uint32_t i = 0; i < directory_size_; i++) {
    auto *entry = directory_[i];
    if (entry == nullptr) {
      continue;
    }
    // Traverse bucket chain, reinserting into new table
    while (entry != nullptr) {
      uint64_t index = entry->hash & new_dir_mask;
      Entry *next = entry->next;
      entry->next = new_dir[index];
      new_dir[index] = entry;
      entry = next;
    }
  }

  // Done. First free the old directory.
  memory_.Free(directory_);

  // Set up the new directory
  directory_size_ = new_dir_size;
  directory_mask_ = new_dir_mask;
  directory_ = new_dir;

  // Update stats
  stats_.num_grows++;
}

HashTable::Entry *HashTable::NewEntry(uint64_t hash) {
  auto entry_size = Entry::Size(key_size_, value_size_);

  // Do we have enough room to store a new entry in the current memory block?
  if (entry_size > available_bytes_) {
    uint64_t block_size = sizeof(MemoryBlock) + (entry_size * kNumBlockElems);
    auto *new_block =
        reinterpret_cast<MemoryBlock *>(memory_.Allocate(block_size));
    new_block->next = block_;
    block_ = new_block;
    next_entry_ = new_block->data;
    available_bytes_ = block_size - sizeof(MemoryBlock);
  }

  // The entry
  auto *entry = new (next_entry_) Entry(hash, nullptr);

  // Bump pointer and reduce available bytes
  next_entry_ += entry_size;
  available_bytes_ -= entry_size;

  // Done
  return entry;
}

void HashTable::TransferMemoryBlocks(HashTable &target) {
  // Find end of our memory block chain
  MemoryBlock *tail = block_;

  // Check if there is anything to transfer
  if (tail == nullptr) {
    return;
  }

  // Move to the end
  while (tail->next != nullptr) {
    tail = tail->next;
  }

  // Transfer everything to the target entry buffer
  do {
    tail->next = target.block_;
  } while (!::peloton::atomic_cas(&target.block_, target.block_, block_));

  // Success
  block_ = nullptr;
  num_elems_ = 0;
  capacity_ = 0;
}

////////////////////////////////////////////////////////////////////////////////
///
/// Scan State
///
////////////////////////////////////////////////////////////////////////////////

HashTable::ScanState::ScanState(const HashTable &table, uint32_t *sel,
                                uint32_t sel_size)
    : table_(table),
      sel_(sel),
      index_(1),
      next_(table_.directory_[0]),
      size_(0),
      sel_size_(sel_size),
      done_(table_.num_elems_ == 0) {
  // Allocate space
  entries_ = static_cast<Entry **>(
      table_.memory_.Allocate(sel_size_ * sizeof(Entry *)));

  // Fill selection vector once since we only populate the entries vector with
  // valid entries
  std::iota(sel_, sel_ + sel_size_, 0);

  // Move scanner
  Next();
}

HashTable::ScanState::~ScanState() {
  if (entries_ != nullptr) {
    table_.memory_.Free(entries_);
    entries_ = nullptr;
  }
}

bool HashTable::ScanState::Init(HashTable::ScanState &scan_state,
                                const HashTable &table, uint32_t *sel,
                                uint32_t sel_size) {
  new (&scan_state) ScanState(table, sel, sel_size);
  return scan_state.done_;
}

void HashTable::ScanState::Destroy(HashTable::ScanState &scan_state) {
  scan_state.~ScanState();
}

bool HashTable::ScanState::Next() {
  // Reset number of active elements
  size_ = 0;

  while (true) {
    // While we're in the middle of a bucket chain and we have room to insert
    // new entries, continue along the bucket chain.
    while (next_ != nullptr && size_ < sel_size_) {
      entries_[size_++] = next_;
      next_ = next_->next;
    }

    // If we've filled up the entries buffer, drop out
    if (size_ == sel_size_) {
      break;
    }

    // If we've exhausted the hash table, drop out
    if (index_ == table_.directory_size_) {
      break;
    }

    // Move to next bucket
    next_ = table_.directory_[index_++];
  }

  // Are we done scanning?
  done_ = (size_ == 0);

  return done_;
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton
