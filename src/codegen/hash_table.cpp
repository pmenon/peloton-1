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
#include "common/exception.h"

namespace peloton {
namespace codegen {

HashTable::HashTable() {
  // This constructor shouldn't generally be used at all, but there are
  // cases when the key-type is not known at construction time.
}

HashTable::HashTable(CodeGen &codegen, const std::vector<type::Type> &key_type,
                     uint32_t value_size)
    : value_size_(value_size) {
  key_storage_.Setup(codegen, key_type);
}

void HashTable::Init(UNUSED_ATTRIBUTE CodeGen &codegen,
                     UNUSED_ATTRIBUTE llvm::Value *ht_ptr) const {
  throw NotImplementedException{
      "Init with no ExecutorContext not supported in HashTable"};
}

void HashTable::Init(CodeGen &codegen, llvm::Value *exec_ctx,
                     llvm::Value *ht_ptr) const {
  auto *key_size = codegen.Const32(key_storage_.MaxStorageSize());
  auto *value_size = codegen.Const32(value_size_);
  codegen.Call(HashTableProxy::Init, {ht_ptr, exec_ctx, key_size, value_size});
}

void HashTable::ProbeOrInsert(CodeGen &codegen, llvm::Value *ht_ptr,
                              llvm::Value *hash,
                              const std::vector<codegen::Value> &key,
                              ProbeCallback &probe_callback,
                              InsertCallback &insert_callback) const {
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
    llvm::Type *ht_entry_type = EntryProxy::GetType(codegen);
    llvm::Value *entry = chain_loop.GetLoopVar(0);

    // Does the hash of the current entry match?
    llvm::Value *entry_hash = codegen.Load(EntryProxy::hash, entry);
    lang::If hash_match(codegen, codegen->CreateICmpEQ(entry_hash, hash_val),
                        "hashMatch");
    {
      // The hashes match, what about the keys?
      llvm::Value *keys_ptr =
          codegen->CreateConstInBoundsGEP2_32(ht_entry_type, entry, 1, 0);

      // Pull out the keys
      std::vector<codegen::Value> hash_entry_keys;
      llvm::Value *values_area =
          key_storage_.LoadValues(codegen, keys_ptr, hash_entry_keys);

      // Check keys for equality
      auto keys_are_equal = Value::TestEquality(codegen, key, hash_entry_keys);
      lang::If key_match(codegen, keys_are_equal.GetValue(), "keyMatch");
      {
        // We found a duplicate key, issue the probe callback
        probe_callback.ProcessEntry(codegen, values_area);
        key_match.EndIf(cont_bb);
      }
      hash_match.EndIf();
    }

    // No match found, move along
    entry = codegen.Load(EntryProxy::next, entry);
    chain_loop.LoopEnd(codegen->CreateICmpNE(entry, null), {entry});
  }

  // No entry found, insert a new one
  llvm::Value *ptr = codegen.Call(HashTableProxy::Insert, {ht_ptr, hash_val});

  // (5)
  llvm::Value *data_space_ptr = key_storage_.StoreValues(codegen, ptr, key);
  insert_callback.StoreValue(codegen, data_space_ptr);

  // Ending block
  codegen->CreateBr(cont_bb);
  codegen->GetInsertBlock()->getParent()->getBasicBlockList().push_back(
      cont_bb);
  codegen->SetInsertPoint(cont_bb);
}

HashTable::ProbeResult HashTable::ProbeOrInsert(
    UNUSED_ATTRIBUTE CodeGen &codegen, UNUSED_ATTRIBUTE llvm::Value *ht_ptr,
    UNUSED_ATTRIBUTE llvm::Value *hash,
    UNUSED_ATTRIBUTE const std::vector<codegen::Value> &key) const {
  throw NotImplementedException{
      "ProbeOrInsert returning probe result not support in HashTable"};
}

void HashTable::Insert(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
                       const std::vector<codegen::Value> &key,
                       InsertCallback &insert_callback) const {
  // Calculate the hash
  llvm::Value *hash_val =
      hash != nullptr ? hash : Hash::HashValues(codegen, key);

  // Get the total size we need
  //  llvm::Value *value_size = insert_callback.GetValueSize(codegen);
  //  llvm::Value *keys_size = codegen.Const32(key_storage_.MaxStorageSize());
  //  llvm::Value *needed_bytes = codegen->CreateAdd(keys_size, value_size);

  // Invoke HashTable::InsertLazy(...)
  llvm::Value *ptr =
      codegen.Call(HashTableProxy::InsertLazy, {ht_ptr, hash_val});

  // Invoke the callback to let her store the payload
  llvm::Value *data_space_ptr = key_storage_.StoreValues(codegen, ptr, key);
  insert_callback.StoreValue(codegen, data_space_ptr);
}

void HashTable::InsertLazy(CodeGen &codegen, llvm::Value *ht_ptr,
                           llvm::Value *hash,
                           const std::vector<codegen::Value> &keys,
                           HashTable::InsertCallback &callback) const {
  Insert(codegen, ht_ptr, hash, keys, callback);
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

void HashTable::Iterate(CodeGen &codegen, llvm::Value *ht_ptr,
                        IterateCallback &callback) const {
  llvm::Value *buckets_ptr = codegen.Load(HashTableProxy::directory, ht_ptr);
  llvm::Value *num_buckets = codegen.Load(HashTableProxy::size, ht_ptr);
  llvm::Value *bucket_num = codegen.Const64(0);
  llvm::Value *bucket_cond = codegen->CreateICmpULT(bucket_num, num_buckets);

  lang::Loop bucket_loop(codegen, bucket_cond, {{"bucketNum", bucket_num}});
  {
    bucket_num = bucket_loop.GetLoopVar(0);
    llvm::Value *bucket =
        codegen->CreateLoad(codegen->CreateGEP(buckets_ptr, bucket_num));
    llvm::Value *null_bucket =
        codegen.NullPtr(llvm::cast<llvm::PointerType>(bucket->getType()));

    lang::Loop chain_loop(codegen, codegen->CreateICmpNE(bucket, null_bucket),
                          {{"entry", bucket}});
    {
      llvm::Type *ht_entry_type = EntryProxy::GetType(codegen);
      llvm::Value *entry = chain_loop.GetLoopVar(0);
      llvm::Value *entry_data =
          codegen->CreateConstInBoundsGEP2_32(ht_entry_type, entry, 1, 0);

      // Pull out keys and invoke callback
      std::vector<codegen::Value> keys;
      auto *data_area_ptr = key_storage_.LoadValues(codegen, entry_data, keys);
      callback.ProcessEntry(codegen, keys, data_area_ptr);

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
      llvm::Value *iter_keys =
          codegen->CreateConstInBoundsGEP1_32(entry_type, entry, 1);
      std::vector<codegen::Value> entry_keys;
      llvm::Value *data_area =
          key_storage_.LoadValues(codegen, iter_keys, entry_keys);

      auto keys_are_equal = Value::TestEquality(codegen, key, entry_keys);
      lang::If key_match(codegen, keys_are_equal.GetValue(), "keyMatch");
      {
        callback.ProcessEntry(codegen, key, data_area);
        key_match.EndIf();
      }
      hash_match.EndIf();
    }
    entry = codegen.Load(EntryProxy::next, entry);
    chain_loop.LoopEnd(codegen->CreateICmpNE(entry, null), {entry});
  }
}

void HashTable::Destroy(CodeGen &codegen, llvm::Value *ht_ptr) const {
  codegen.Call(HashTableProxy::Destroy, {ht_ptr});
}

namespace {

/**
 * This class enables random-access to a (fixed, flattened view) of a hash
 * table. It is used during vectorized iteration over the hash table. Hence,
 * the probed indices always fall within the active vector, usually in the range
 * [0-1024).
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
  auto *done =
      codegen.Call(ScanStateProxy::Init,
                   {scan_state, ht_ptr, selection_vector.GetVectorPtr(),
                    codegen.Const32(selection_vector.GetCapacity())});

  // Table accessor
  llvm::Value *entries = codegen.Load(ScanStateProxy::entries, scan_state);
  TableAccess table_access(key_storage_, entries);

  // Loop while the scan state is valid
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