//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table.cpp
//
// Identification: src/codegen/hash_table.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/hash_table.h"

#include "codegen/hash.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/hash_table_proxy.h"
#include "codegen/type/integer_type.h"
#include "common/exception.h"
#include "type/type_id.h"

namespace peloton {
namespace codegen {

// The global attribute information instance used to populate a row's hash value
const planner::AttributeInfo HashTable::kHashAI{type::Integer::Instance(), 0,
                                                "hash"};

HashTable::HashTable() {
  // This constructor shouldn't generally be used at all, but there are
  // cases when the key-type is not known at construction time.
}

HashTable::HashTable(CodeGen &codegen, const std::vector<type::Type> &key_type,
                     uint32_t value_size)
    : value_size_(value_size) {
  key_storage_.Setup(codegen, key_type);
}

void HashTable::Init(CodeGen &codegen, llvm::Value *exec_ctx,
                     llvm::Value *ht_ptr) const {
  auto *key_size = codegen.Const32(key_storage_.MaxStorageSize());
  auto *value_size = codegen.Const32(value_size_);
  codegen.Call(HashTableProxy::Init, {ht_ptr, exec_ctx, key_size, value_size});
}

llvm::Value *HashTable::KeysForEntry(CodeGen &codegen, llvm::Value *entry,
                                     std::vector<codegen::Value> &key) const {
  // Compute a pointer to the key space
  llvm::Value *key_space = codegen->CreateConstInBoundsGEP1_32(
      EntryProxy::GetType(codegen), entry, 1);

  // Deserialize the key values
  return key_storage_.LoadValues(codegen, key_space, key);
}

void HashTable::ProbeOrInsert(CodeGen &codegen, llvm::Value *ht_ptr,
                              llvm::Value *hash,
                              const std::vector<codegen::Value> &key,
                              InsertMode insert_mode,
                              ProbeCallback *probe_callback,
                              InsertCallback *insert_callback) const {
  llvm::BasicBlock *cont_bb =
      llvm::BasicBlock::Create(codegen.GetContext(), "cont");

  // Compute hash value
  llvm::Value *hash_val =
      hash != nullptr ? hash : Hash::HashValues(codegen, key);

  // Compute bucket position
  llvm::Value *mask = codegen.Load(HashTableProxy::mask, ht_ptr);
  llvm::Value *bucket_idx = codegen->CreateAnd(hash_val, mask);
  llvm::Value *directory = codegen.Load(HashTableProxy::directory, ht_ptr);
  llvm::Value *bucket =
      codegen->CreateLoad(codegen->CreateInBoundsGEP(directory, {bucket_idx}));

  // Iterate the bucket chain until we get a NULL entry
  llvm::Value *null =
      codegen.NullPtr(llvm::cast<llvm::PointerType>(bucket->getType()));
  llvm::Value *end_condition = codegen->CreateICmpNE(bucket, null);
  lang::Loop chain_loop(codegen, end_condition, {{"iter", bucket}});
  {
    // The current entry
    llvm::Value *entry = chain_loop.GetLoopVar(0);

    // Does the hash of the current entry match?
    llvm::Value *entry_hash = codegen.Load(EntryProxy::hash, entry);
    lang::If hash_match(codegen, codegen->CreateICmpEQ(entry_hash, hash_val),
                        "hashMatch");
    {
      // The hashes match, what about the keys?
      std::vector<codegen::Value> hash_entry_keys;
      llvm::Value *values_area = KeysForEntry(codegen, entry, hash_entry_keys);

      // Check keys for equality
      auto keys_are_equal = Value::TestEquality(codegen, key, hash_entry_keys);
      lang::If key_match(codegen, keys_are_equal.GetValue(), "keyMatch");
      {
        // We found a duplicate key, issue the probe callback
        if (probe_callback != nullptr) {
          probe_callback->ProcessEntry(codegen, values_area);
        }
        key_match.EndIf(cont_bb);
      }
      hash_match.EndIf();
    }

    // No match found, move along
    entry = codegen.Load(EntryProxy::next, entry);
    chain_loop.LoopEnd(codegen->CreateICmpNE(entry, null), {entry});
  }

  // No entry found, insert a new one
  llvm::Value *ptr = nullptr;
  switch (insert_mode) {
    case InsertMode::Normal: {
      ptr = codegen.Call(HashTableProxy::Insert, {ht_ptr, hash_val});
      break;
    }
    case InsertMode::Partitioned: {
      ptr = codegen.Call(HashTableProxy::InsertPartitioned, {ht_ptr, hash_val});
      break;
    }
    default: {
      throw Exception(
          "Lazy insertions not supported in ProbeOrInsert. Are you sure you "
          "know what you're doing?");
    }
  };

  llvm::Value *value_space_ptr = key_storage_.StoreValues(codegen, ptr, key);
  if (insert_callback != nullptr) {
    insert_callback->StoreValue(codegen, value_space_ptr);
  }

  // Ending block
  codegen->CreateBr(cont_bb);
  codegen->GetInsertBlock()->getParent()->getBasicBlockList().push_back(
      cont_bb);
  codegen->SetInsertPoint(cont_bb);
}

void HashTable::Insert(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
                       const std::vector<codegen::Value> &key,
                       HashTable::InsertMode mode,
                       HashTable::InsertCallback &callback) const {
  // Calculate the hash
  llvm::Value *hash_val =
      hash != nullptr ? hash : Hash::HashValues(codegen, key);

  llvm::Value *ptr = nullptr;
  switch (mode) {
    case HashTable::InsertMode::Normal: {
      ptr = codegen.Call(HashTableProxy::Insert, {ht_ptr, hash_val});
      break;
    }
    case HashTable::InsertMode::Lazy: {
      ptr = codegen.Call(HashTableProxy::InsertLazy, {ht_ptr, hash_val});
      break;
    }
    case HashTable::InsertMode::Partitioned: {
      ptr = codegen.Call(HashTableProxy::InsertPartitioned, {ht_ptr, hash_val});
      break;
    }
  }

  // Invoke the callback to let her store the payload
  llvm::Value *data_space_ptr = key_storage_.StoreValues(codegen, ptr, key);
  callback.StoreValue(codegen, data_space_ptr);
}

void HashTable::BuildLazy(CodeGen &codegen, llvm::Value *ht_ptr) const {
  codegen.Call(HashTableProxy::BuildLazy, {ht_ptr});
}

void HashTable::ReserveLazy(CodeGen &codegen, llvm::Value *ht_ptr,
                            llvm::Value *thread_states,
                            uint32_t ht_state_offset) const {
  codegen.Call(HashTableProxy::ReserveLazy,
               {ht_ptr, thread_states, codegen.Const32(ht_state_offset)});
}

void HashTable::MergeLazyUnfinished(CodeGen &codegen, llvm::Value *global_ht,
                                    llvm::Value *local_ht) const {
  codegen.Call(HashTableProxy::MergeLazyUnfinished, {global_ht, local_ht});
}

void HashTable::MergePartitionRange(CodeGen &codegen,
                                    llvm::Value *target_ht_ptr,
                                    llvm::Value *part_list,
                                    llvm::Value *begin_idx,
                                    llvm::Value *end_idx,
                                    HashTable::MergeCallback &callback) const {
  /*
   * This function implements the most complex operation on Peloton hash tables.
   * We decompose the process using functors/lambas below for ease of
   * readability. These functions aren't extracted as generic helper functions
   * because they are not generic, but rather very specific to this operation.
   *
   * At a high-level, we implement this:
   *
   * def insert_new_entry(t, entry, bucket):
   *   if needsGrow(t):
   *     grow(t)
   *     merge_entry_into_bucket(t, entry, bucketFor(t, entry))
   *   else:
   *     t.size++
   *     entry.next = bucket
   *     bucket = entry
   *
   * def merge_entry_into_bucket(t, entry, bucket):
   *   for bucketEntry in bucket:
   *     if bucketEntry.hash == entry.hash && keyEq(bucketEntry, entry):
   *       merge(bucketEntry, entry)
   *       return
   *   insert_new_entry(t, entry, bucket)
   *
   * def merge_partition(t, p):
   *   for entry in p:
   *     bucket = bucketFor(t, entry)
   *     merge_entry_into_bucket(t, entry, bucket)
   *
   * for p in partitions:
   *   merge_partition(t, p)
   *
   */

  llvm::BasicBlock *found_bb =
      llvm::BasicBlock::Create(codegen.GetContext(), "found");

  llvm::BasicBlock *grow_bb =
      llvm::BasicBlock::Create(codegen.GetContext(), "grow");

  llvm::Value *directory_ptr = nullptr;
  llvm::Value *mask_ptr = nullptr;
  llvm::Value *num_elems_ptr = nullptr;
  llvm::Value *capacity_ptr = nullptr;

  /*
   * This function inserts a new Entry into the hash table, potentially growing
   * it and retrying the insertion process.
   */

  const auto insert_new_entry =
      [&codegen, &target_ht_ptr, &grow_bb, &capacity_ptr, &num_elems_ptr](
          llvm::Value *entry, llvm::Value *bucket_head_ptr,
          llvm::Value *bucket_head) {
        llvm::Value *capacity = codegen->CreateLoad(capacity_ptr);
        llvm::Value *num_elems = codegen->CreateLoad(num_elems_ptr);
        num_elems = codegen->CreateAdd(num_elems, codegen.Const64(1));

        lang::If should_grow_ht(codegen,
                                codegen->CreateICmpUGE(num_elems, capacity));
        {
          // Grow
          codegen.Call(HashTableProxy::Grow, {target_ht_ptr});
        }
        should_grow_ht.EndIf(grow_bb);

        // Link no grow
        codegen->CreateStore(num_elems, num_elems_ptr);
        codegen.Store(EntryProxy::next, entry, bucket_head);
        codegen->CreateStore(entry, bucket_head_ptr);
      };

  /*
   * This function merges a single entry into the target table, potentially
   * growing the table if necessary.
   */

  const auto merge_entry_into_bucket = [this, &codegen, &callback, &found_bb,
                                        &insert_new_entry](
                                           llvm::Value *entry,
                                           llvm::Value *bucket_head_ptr) {
    llvm::Value *null_entry =
        codegen.NullPtr(EntryProxy::GetType(codegen)->getPointerTo());

    // Load the hash value, keys and payload for the partition entry
    llvm::Value *entry_hash = codegen.Load(EntryProxy::hash, entry);

    std::vector<codegen::Value> part_entry_keys;
    llvm::Value *entry_payload = KeysForEntry(codegen, entry, part_entry_keys);

    // Loop over all entries in the bucket
    llvm::Value *bucket_head = codegen->CreateLoad(bucket_head_ptr);
    lang::Loop chain_loop(codegen,
                          codegen->CreateICmpNE(bucket_head, null_entry),
                          {{"hashIter", bucket_head}});
    {
      llvm::Value *bucket_entry = chain_loop.GetLoopVar(0);

      // Are hashes equal?
      llvm::Value *bucket_entry_hash =
          codegen.Load(EntryProxy::hash, bucket_entry);
      llvm::Value *hash_eq =
          codegen->CreateICmpEQ(bucket_entry_hash, entry_hash);
      lang::If hash_match(codegen, hash_eq, "hashMatch");
      {
        // Are keys equal?
        std::vector<codegen::Value> bucket_entry_keys;
        llvm::Value *bucket_entry_payload =
            KeysForEntry(codegen, bucket_entry, bucket_entry_keys);

        codegen::Value key_eq = codegen::Value::TestEquality(
            codegen, bucket_entry_keys, part_entry_keys);

        lang::If keys_match(codegen, key_eq.GetValue(), "keyMatch");
        {
          // Full match. Perform merge.
          callback.MergeValues(codegen, bucket_entry_payload, entry_payload);
        }
        keys_match.EndIf(found_bb);
      }
      hash_match.EndIf();

      // Move along
      bucket_entry = codegen.Load(EntryProxy::next, bucket_entry);
      chain_loop.LoopEnd(codegen->CreateICmpNE(bucket_entry, null_entry),
                         {bucket_entry});

      // Not in bucket, insert
      insert_new_entry(entry, bucket_head_ptr, bucket_head);
    }
  };

  /*
   * This function merges all entries stored in the input partition into the
   * target hash table.
   */

  const auto merge_partition = [&codegen, &directory_ptr, &mask_ptr, &found_bb,
                                &grow_bb, &merge_entry_into_bucket](
                                   llvm::Value *partition) {
    llvm::Value *null_entry =
        codegen.NullPtr(EntryProxy::GetType(codegen)->getPointerTo());

    llvm::Value *entry = partition;
    lang::Loop loop_partition(codegen, codegen->CreateICmpNE(entry, null_entry),
                              {{"entry", entry}});
    {
      entry = loop_partition.GetLoopVar(0);
      llvm::Value *next = codegen.Load(EntryProxy::next, entry);

      // Jump to this block in order to support the grow path
      codegen->CreateBr(grow_bb);
      codegen->GetInsertBlock()->getParent()->getBasicBlockList().push_back(
          grow_bb);
      codegen->SetInsertPoint(grow_bb);

      llvm::Value *directory = codegen->CreateLoad(directory_ptr);
      llvm::Value *mask = codegen->CreateLoad(mask_ptr);

      // Compute bucket position
      llvm::Value *entry_hash = codegen.Load(EntryProxy::hash, entry);
      llvm::Value *bucket_idx = codegen->CreateAnd(entry_hash, mask);

      llvm::Value *bucket_head_ptr =
          codegen->CreateInBoundsGEP(directory, {bucket_idx});

      // Merge this entry into the hash table bucket
      merge_entry_into_bucket(entry, bucket_head_ptr);

      // Ending block
      codegen->CreateBr(found_bb);
      codegen->GetInsertBlock()->getParent()->getBasicBlockList().push_back(
          found_bb);
      codegen->SetInsertPoint(found_bb);

      loop_partition.LoopEnd(codegen->CreateICmpNE(next, null_entry), {next});
    }
  };

  /*
   * This section loops over the partition range [begin_idx, end_idx) and merges
   * each whole partition into the target hash table.
   */

  llvm::Value *idx = begin_idx;
  lang::Loop loop_partitions(codegen, codegen->CreateICmpULT(idx, end_idx),
                             {{"index", idx}});
  {
    idx = loop_partitions.GetLoopVar(0);

    // Load member pointers earlier one because they are reused later
    directory_ptr = codegen.AddressOf(HashTableProxy::directory, target_ht_ptr);
    mask_ptr = codegen.AddressOf(HashTableProxy::mask, target_ht_ptr);
    num_elems_ptr = codegen.AddressOf(HashTableProxy::num_elems, target_ht_ptr);
    capacity_ptr = codegen.AddressOf(HashTableProxy::capacity, target_ht_ptr);

    // Load the start of the partition
    llvm::Value *partition =
        codegen->CreateLoad(codegen->CreateInBoundsGEP(part_list, {idx}));

    // Merge the entire partition
    merge_partition(partition);

    // Move along
    idx = codegen->CreateAdd(idx, codegen.Const64(1));
    loop_partitions.LoopEnd(codegen->CreateICmpULT(idx, end_idx), {idx});
  }
}

void HashTable::TransferPartitions(CodeGen &codegen, llvm::Value *ht_ptr,
                                   llvm::Value *thread_states,
                                   uint32_t tl_ht_state_offset,
                                   llvm::Function *merging_func) const {
  std::vector<llvm::Value *> args = {
      // The global hash table we'll transfer all thread-local data to
      ht_ptr,
      // A pointer to the ThreadStates object containing all thread-local state
      thread_states,
      // The offset into each thead's state object where the hash table is
      codegen.Const32(tl_ht_state_offset),
      // The merging function used to merge multiple entries together
      codegen->CreatePointerCast(
          merging_func,
          proxy::TypeBuilder<
              codegen::util::HashTable::MergingFunction>::GetType(codegen))};

  // Call
  codegen.Call(HashTableProxy::TransferPartitions, args);
}

void HashTable::FinishPartitions(CodeGen &codegen, llvm::Value *ht_ptr,
                                 llvm::Value *query_state) const {
  std::vector<llvm::Value *> args = {
      ht_ptr, codegen->CreatePointerCast(query_state, codegen.VoidPtrType())};

  // Call
  codegen.Call(HashTableProxy::FinishPartitions, args);
}

void HashTable::Repartition(CodeGen &codegen, llvm::Value *ht_ptr) const {
  codegen.Call(HashTableProxy::Repartition, {ht_ptr});
}

void HashTable::MergePartitions(CodeGen &codegen, llvm::Value *ht_ptr,
                                llvm::Value *query_state,
                                llvm::Value *target_ht_ptr,
                                llvm::Function *merging_func) const {
  std::vector<llvm::Value *> args = {
      // The hash table pointer whose contents we wish to merge into the target
      ht_ptr,
      // The query state object, casted to a void pointer. We do this because
      // it is a runtime generated type that pre-compiled C++ code doesn't know
      // about.
      codegen->CreatePointerCast(query_state, codegen.VoidPtrType()),
      // The target table that accepts new data
      target_ht_ptr,
      // The function used to perform the merging of entries in the source table
      // with the target table
      codegen->CreatePointerCast(
          merging_func,
          proxy::TypeBuilder<util::HashTable::MergeEntryFunction>::GetType(
              codegen))};

  // Call
  codegen.Call(HashTableProxy::MergePartitions, args);
}

void HashTable::Merge(CodeGen &codegen, llvm::Value *dest_ht_ptr,
                      llvm::Value *src_ht_ptr,
                      HashTable::MergeCallback &callback,
                      UNUSED_ATTRIBUTE bool multithreaded) const {
  // TODO: Make multithreaded
  llvm::Value *begin_idx = codegen.Const64(0);
  llvm::Value *end_idx = codegen.Load(HashTableProxy::dir_size, src_ht_ptr);
  llvm::Value *part_list = codegen.Load(HashTableProxy::directory, src_ht_ptr);
  MergePartitionRange(codegen, dest_ht_ptr, part_list, begin_idx, end_idx,
                      callback);
}

void HashTable::Iterate(CodeGen &codegen, llvm::Value *ht_ptr,
                        IterateCallback &callback) const {
  llvm::Value *directory = codegen.Load(HashTableProxy::directory, ht_ptr);
  llvm::Value *num_buckets = codegen.Load(HashTableProxy::dir_size, ht_ptr);
  llvm::Value *bucket_num = codegen.Const64(0);
  llvm::Value *bucket_cond = codegen->CreateICmpULT(bucket_num, num_buckets);

  // Iterate over all buckets in directory
  lang::Loop bucket_loop(codegen, bucket_cond, {{"bucketNum", bucket_num}});
  {
    bucket_num = bucket_loop.GetLoopVar(0);

    llvm::Value *bucket = codegen->CreateLoad(
        codegen->CreateInBoundsGEP(directory, {bucket_num}));

    llvm::Value *null_bucket =
        codegen.NullPtr(llvm::cast<llvm::PointerType>(bucket->getType()));

    // Iterate over the bucket's chain
    lang::Loop chain_loop(codegen, codegen->CreateICmpNE(bucket, null_bucket),
                          {{"entry", bucket}});
    {
      llvm::Value *entry = chain_loop.GetLoopVar(0);

      // Pull out keys and compute payload position
      std::vector<codegen::Value> keys;
      llvm::Value *data_area_ptr = KeysForEntry(codegen, entry, keys);

      callback.ProcessEntry(codegen, entry, keys, data_area_ptr);

      // Move along chain
      entry = codegen.Load(EntryProxy::next, entry);
      chain_loop.LoopEnd(codegen->CreateICmpNE(entry, null_bucket), {entry});
    }

    // Move to next bucket
    bucket_num = codegen->CreateAdd(bucket_num, codegen.Const64(1));
    bucket_loop.LoopEnd(codegen->CreateICmpULT(bucket_num, num_buckets),
                        {bucket_num});
  }
}

void HashTable::FindAll(CodeGen &codegen, llvm::Value *ht_ptr,
                        const std::vector<codegen::Value> &key,
                        IterateCallback &callback) const {
  llvm::Value *hash = Hash::HashValues(codegen, key);

  llvm::Value *mask = codegen.Load(HashTableProxy::mask, ht_ptr);
  llvm::Value *bucket_idx = codegen->CreateAnd(hash, mask);
  llvm::Value *directory = codegen.Load(HashTableProxy::directory, ht_ptr);
  llvm::Value *bucket =
      codegen->CreateLoad(codegen->CreateInBoundsGEP(directory, {bucket_idx}));

  llvm::Type *entry_type = EntryProxy::GetType(codegen);
  llvm::Value *null = codegen.NullPtr(entry_type->getPointerTo());

  // Loop chain
  llvm::Value *end_condition = codegen->CreateICmpNE(bucket, null);
  lang::Loop chain_loop(codegen, end_condition, {{"iter", bucket}});
  {
    llvm::Value *entry = chain_loop.GetLoopVar(0);
    llvm::Value *entry_hash = codegen.Load(EntryProxy::hash, entry);
    lang::If hash_match(codegen, codegen->CreateICmpEQ(entry_hash, hash),
                        "hashMatch");
    {
      std::vector<codegen::Value> entry_keys;
      llvm::Value *data_area = KeysForEntry(codegen, entry, entry_keys);

      auto keys_are_equal = Value::TestEquality(codegen, key, entry_keys);
      lang::If key_match(codegen, keys_are_equal.GetValue(), "keyMatch");
      {
        // Found match
        callback.ProcessEntry(codegen, entry, key, data_area);
      }
      key_match.EndIf();
    }
    hash_match.EndIf();

    entry = codegen.Load(EntryProxy::next, entry);
    chain_loop.LoopEnd(codegen->CreateICmpNE(entry, null), {entry});
  }
}

void HashTable::Destroy(CodeGen &codegen, llvm::Value *ht_ptr) const {
  codegen.Call(HashTableProxy::Destroy, {ht_ptr});
}

namespace {

/**
 * This class enables random-access to a fixed, flattened view of a bucket-
 * chained table. It is used during vectorized iteration over the hash table.
 */
class TableAccess : public HashTable::HashTableAccess {
 public:
  TableAccess(const CompactStorage &key_storage, llvm::Value *entries)
      : key_storage_(key_storage), entries_(entries), entry_(nullptr) {}

  /**
   * Load a pointer to the entry at the given index within this view.
   *
   * @param codegen The codegen instance.
   * @param index The index of the entry in this view whose pointer to load.
   * @return A pointer to the entry.
   */
  llvm::Value *LoadEntry(CodeGen &codegen, llvm::Value *index) const {
    if (entry_ == nullptr) {
      entry_ = codegen->CreateLoad(codegen->CreateGEP(entries_, index));
    }
    return entry_;
  }

  /**
   * Extract the key stored at the entry at the given index.
   *
   * @param codegen The codegen instance
   * @param index The index within the view whose entry's key to load
   * @param[out] keys The vector where we insert key values
   */
  void ExtractBucketKeys(CodeGen &codegen, llvm::Value *index,
                         std::vector<codegen::Value> &keys) const override {
    // Load the Entry* at the given index
    llvm::Value *entry = LoadEntry(codegen, index);

    // Compute a pointer to the key space
    llvm::Value *key_space = codegen->CreateConstInBoundsGEP1_32(
        EntryProxy::GetType(codegen), entry, 1);

    // Deserialize the key values
    key_storage_.LoadValues(codegen, key_space, keys);
  }

  /**
   * Return a pointer to the payload for the entry at the given index in our
   * view.
   *
   * @param codegen The codegen instance
   * @param index The index within the view whose payload pointer to compute
   * @return A pointer to where the entry's payload is serialized
   */
  llvm::Value *BucketValue(CodeGen &codegen,
                           llvm::Value *index) const override {
    // Load the Entry* at the given index
    llvm::Value *entry = LoadEntry(codegen, index);

    // Compute a pointer to the value space by computing the key space and
    // skipping the key size
    llvm::Value *key_space = codegen->CreateConstInBoundsGEP1_32(
        EntryProxy::GetType(codegen), entry, 1);

    key_space = codegen->CreatePointerCast(key_space, codegen.CharPtrType());

    return codegen->CreateConstInBoundsGEP1_32(codegen.ByteType(), key_space,
                                               key_storage_.MaxStorageSize());
  }

 private:
  // The storage format
  const CompactStorage &key_storage_;

  // The entries directory. This is a view over the hash table.
  llvm::Value *entries_;

  // The entry we're interested. It's mutable because it is assigned from a
  // const method. This isn't ideal, but okay for now.
  mutable llvm::Value *entry_;
};

}  // namespace

void HashTable::VectorizedIterate(
    CodeGen &codegen, llvm::Value *ht_ptr, Vector &selection_vector,
    HashTable::VectorizedIterateCallback &callback) const {
  // Create a hash table ScanState object
  auto *scan_state =
      codegen.AllocateVariable(ScanStateProxy::GetType(codegen), "htScanState");

  // Initialize it
  codegen.Call(ScanStateProxy::Init,
               {scan_state, ht_ptr, selection_vector.GetVectorPtr(),
                codegen.Const32(selection_vector.GetCapacity())});

  // Table accessor
  llvm::Value *entries = codegen.Load(ScanStateProxy::entries, scan_state);
  TableAccess table_access(key_storage_, entries);

  // Loop while the scan state is valid
  llvm::Value *done = codegen.Call(ScanStateProxy::Next, {scan_state});
  lang::Loop loop(codegen, codegen->CreateNot(done), {});
  {
    auto *num_elems = codegen.Load(ScanStateProxy::size, scan_state);
    selection_vector.SetNumElements(num_elems);

    // Issue callback
    callback.ProcessEntries(codegen, codegen.Const32(0), num_elems,
                            selection_vector, table_access);

    done = codegen.Call(ScanStateProxy::Next, {scan_state});
    loop.LoopEnd(codegen->CreateNot(done), {});
  }

  // Cleanup
  codegen.Call(ScanStateProxy::Destroy, {scan_state});
}

}  // namespace codegen
}  // namespace peloton