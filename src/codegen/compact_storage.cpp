//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compact_storage.cpp
//
// Identification: src/codegen/compact_storage.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/compact_storage.h"

#include <algorithm>

#include "codegen/type/sql_type.h"

namespace peloton {
namespace codegen {

// TODO: Only load/store values if it's not NULL

////////////////////////////////////////////////////////////////////////////////
///
/// Bitmap Writer
///
////////////////////////////////////////////////////////////////////////////////

class CompactStorage::BitmapWriter {
 public:
  /**
   *
   * @param codegen
   * @param storage
   * @param storage_ptr
   */
  BitmapWriter(CodeGen &codegen, const CompactStorage &storage,
               llvm::Value *storage_ptr)
      : bitmap_ptr_(nullptr) {
    // Sanity check to make sure the storage pointer we get is correctly typed
    PELOTON_ASSERT(
        llvm::isa<llvm::PointerType>(storage_ptr->getType()) &&
        llvm::cast<llvm::PointerType>(storage_ptr->getType()->getScalarType())
                ->getElementType() == storage.GetStorageType());

    // Compute a pointer to the bitmap
    auto null_bitmap_pos =
        static_cast<uint32_t>(storage.storage_format_.size());
    auto *bitmap_arr = codegen->CreateConstInBoundsGEP2_32(
        storage.GetStorageType(), storage_ptr, 0, null_bitmap_pos);

    // Index into the first element, treating it as a char *
    bitmap_ptr_ = codegen->CreateConstInBoundsGEP2_32(
        storage.GetNullBitmapType(), bitmap_arr, 0, 0);

    uint32_t num_bytes = (storage.GetNumElements() + 7) >> 3u;
    bytes_.resize(num_bytes, nullptr);
  }

  /**
   *
   * @param codegen
   * @param bit_idx
   * @param bit_val
   */
  void SetBit(CodeGen &codegen, uint32_t bit_idx, llvm::Value *bit_val) {
    PELOTON_ASSERT(bit_val->getType() == codegen.BoolType());
    // Cast to byte, left shift into position
    auto *byte_val = codegen->CreateZExt(bit_val, codegen.ByteType());
    byte_val = codegen->CreateShl(byte_val, bit_idx & 7u);

    // Store in bytes
    uint32_t byte_pos = bit_idx >> 3u;
    bytes_[byte_pos] = (bytes_[byte_pos] == nullptr)
                           ? byte_val
                           : codegen->CreateOr(bytes_[byte_pos], byte_val);
  }

  /**
   *
   * @param codegen
   */
  void Write(CodeGen &codegen) const {
    for (uint32_t idx = 0; idx < bytes_.size(); idx++) {
      llvm::Value *addr = codegen->CreateConstInBoundsGEP1_32(
          codegen.ByteType(), bitmap_ptr_, idx);
      if (bytes_[idx] != nullptr) {
        codegen->CreateStore(bytes_[idx], addr);
      } else {
        codegen->CreateStore(codegen.Const8(0), addr);
      }
    }
  }

 private:
  // A pointer to the bitmap
  llvm::Value *bitmap_ptr_;

  // The bytes that compose the bitmap
  std::vector<llvm::Value *> bytes_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Bitmap Reader
///
////////////////////////////////////////////////////////////////////////////////

class CompactStorage::BitmapReader {
 public:
  /**
   *
   * @param codegen
   * @param storage
   * @param storage_ptr
   */
  BitmapReader(CodeGen &codegen, const CompactStorage &storage,
               llvm::Value *storage_ptr)
      : bitmap_ptr_(nullptr) {
    // Sanity check to make sure the storage pointer we get is correctly typed
    PELOTON_ASSERT(
        llvm::isa<llvm::PointerType>(storage_ptr->getType()) &&
        llvm::cast<llvm::PointerType>(storage_ptr->getType()->getScalarType())
                ->getElementType() == storage.GetStorageType());

    // Compute a pointer to the bitmap
    auto null_bitmap_pos =
        static_cast<uint32_t>(storage.storage_format_.size());
    auto *bitmap_arr = codegen->CreateConstInBoundsGEP2_32(
        storage.GetStorageType(), storage_ptr, 0, null_bitmap_pos);

    // Index into the first element, treating it as a char *
    bitmap_ptr_ = codegen->CreateConstInBoundsGEP2_32(
        storage.GetNullBitmapType(), bitmap_arr, 0, 0);

    uint32_t num_bytes = (storage.GetNumElements() + 7) >> 3u;
    bytes_.resize(num_bytes, nullptr);
  }

  /**
   *
   * @param codegen
   * @param bit_idx
   * @return
   */
  llvm::Value *GetBit(CodeGen &codegen, uint32_t bit_idx) {
    uint32_t byte_pos = bit_idx >> 3u;

    if (bytes_[byte_pos] == nullptr) {
      // Load the byte
      auto *byte_addr = codegen->CreateConstInBoundsGEP1_32(
          codegen.ByteType(), bitmap_ptr_, byte_pos);
      bytes_[byte_pos] = codegen->CreateLoad(byte_addr);
    }

    // Pull out only the bit we want
    auto *mask = codegen.Const8(1u << (bit_idx & 7));
    auto *masked_byte = codegen->CreateAnd(bytes_[byte_pos], mask);

    // Return if it equals 1
    return codegen->CreateICmpNE(masked_byte, codegen.Const8(0));
  }

 private:
  // A pointer to the bitmap
  llvm::Value *bitmap_ptr_;

  // The bytes that compose the bitmap
  std::vector<llvm::Value *> bytes_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Compact Storage
///
////////////////////////////////////////////////////////////////////////////////

CompactStorage::CompactStorage()
    : storage_type_(nullptr), storage_size_(0), null_bitmap_type_(nullptr) {}

llvm::Type *CompactStorage::Setup(CodeGen &codegen,
                                  const std::vector<type::Type> &types) {
  // Return the constructed type if the compact storage has already been set up
  if (storage_type_ != nullptr) {
    return storage_type_;
  }

  // Copy over the types for convenience
  schema_ = types;

  // Add tracking metadata for all data elements that will be stored
  for (uint32_t i = 0; i < schema_.size(); i++) {
    const auto &sql_type = schema_[i].GetSqlType();

    llvm::Type *val_type = nullptr;
    llvm::Type *len_type = nullptr;
    sql_type.GetTypeForMaterialization(codegen, val_type, len_type);

    /*
     * Create a slot metadata entry for the value
     *
     * Note: The physical and logical index are the same for now. The physical
     *       index is modified after storage format optimization (later).
     */
    storage_format_.emplace_back(
        EntryInfo{.type = val_type,
                  .physical_index = i,
                  .logical_index = i,
                  .is_length = false,
                  .num_bytes = codegen.SizeOf(val_type)});

    // If there is a length component, add that too
    if (len_type != nullptr) {
      storage_format_.emplace_back(
          EntryInfo{.type = len_type,
                    .physical_index = i,
                    .logical_index = i,
                    .is_length = true,
                    .num_bytes = codegen.SizeOf(len_type)});
    }
  }

  /*
   * Sort the entries by decreasing size. This minimizes storage overhead due to
   * padding (potentially) added by LLVM.
   */
  std::sort(storage_format_.begin(), storage_format_.end(),
            [](const EntryInfo &left, const EntryInfo &right) {
              return right.num_bytes < left.num_bytes;
            });

  // Now we construct the LLVM type of this storage space
  std::vector<llvm::Type *> llvm_types;

  for (uint32_t idx = 0; idx < storage_format_.size(); idx++) {
    llvm_types.push_back(storage_format_[idx].type);
    storage_format_[idx].physical_index = idx;
  }

  auto num_null_bytes = static_cast<uint32_t>((schema_.size() + 7) >> 3u);
  null_bitmap_type_ = codegen.ArrayType(codegen.ByteType(), num_null_bytes);
  llvm_types.push_back(null_bitmap_type_);

  // Construct the finalized types
  storage_type_ = llvm::StructType::get(codegen.GetContext(), llvm_types, true);
  storage_size_ = static_cast<uint32_t>(codegen.SizeOf(storage_type_));
  return storage_type_;
}

llvm::Value *CompactStorage::StoreValues(
    CodeGen &codegen, llvm::Value *area_start,
    const std::vector<codegen::Value> &to_store) const {
  PELOTON_ASSERT(storage_type_ != nullptr);
  PELOTON_ASSERT(to_store.size() == schema_.size());

  // Decompose the values we're storing into their raw value, length and
  // null-bit components
  const auto num_elems = static_cast<uint32_t>(schema_.size());

  std::vector<llvm::Value *> vals(num_elems);
  std::vector<llvm::Value *> lengths(num_elems);
  std::vector<llvm::Value *> nulls(num_elems);

  for (uint32_t i = 0; i < num_elems; i++) {
    to_store[i].ValuesForMaterialization(codegen, vals[i], lengths[i],
                                         nulls[i]);
  }

  // Cast the area pointer to our constructed type
  auto *typed_ptr =
      codegen->CreateBitCast(area_start, storage_type_->getPointerTo());

  // The NULL bitmap
  BitmapWriter null_bitmap(codegen, *this, typed_ptr);

  // Fill in the actual values
  for (const auto &entry_info : storage_format_) {
    // Load the address where this entry's data is in the storage space
    llvm::Value *addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, entry_info.physical_index);

    // Load it
    if (entry_info.is_length) {
      codegen->CreateStore(lengths[entry_info.logical_index], addr);
    } else {
      codegen->CreateStore(vals[entry_info.logical_index], addr);

      // Update the bitmap
      null_bitmap.SetBit(codegen, entry_info.logical_index,
                         nulls[entry_info.logical_index]);
    }
  }

  // Write the NULL bitmap
  null_bitmap.Write(codegen);

  // Return a pointer into the space just after all the entries we just wrote
  return codegen->CreateConstInBoundsGEP1_32(codegen.ByteType(), area_start,
                                             storage_size_);
}

llvm::Value *CompactStorage::LoadValues(
    CodeGen &codegen, llvm::Value *area_start,
    std::vector<codegen::Value> &output) const {
  const auto num_elems = static_cast<uint32_t>(schema_.size());

  std::vector<llvm::Value *> vals(num_elems);
  std::vector<llvm::Value *> lengths(num_elems);
  std::vector<llvm::Value *> nulls(num_elems);

  auto *typed_ptr =
      codegen->CreateBitCast(area_start, storage_type_->getPointerTo());

  // The NULL bitmap
  BitmapReader null_bitmap(codegen, *this, typed_ptr);

  /*
   * Collect all the values in the provided storage space, separating the values
   * into either value components or length components
   */
  for (const auto &entry_info : storage_format_) {
    // Load the raw value
    llvm::Value *entry_addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, entry_info.physical_index);
    llvm::Value *entry = codegen->CreateLoad(entry_addr);

    // Set the length or value component
    if (entry_info.is_length) {
      lengths[entry_info.logical_index] = entry;
    } else {
      vals[entry_info.logical_index] = entry;

      // Load the null-bit too
      nulls[entry_info.logical_index] =
          null_bitmap.GetBit(codegen, entry_info.logical_index);
    }
  }

  // Create the values
  output.resize(num_elems);
  for (uint64_t i = 0; i < num_elems; i++) {
    output[i] = codegen::Value::ValueFromMaterialization(schema_[i], vals[i],
                                                         lengths[i], nulls[i]);
  }

  // Return a pointer into the space just after all the entries we stored
  return codegen->CreateConstInBoundsGEP1_32(
      codegen.ByteType(),
      codegen->CreateBitCast(area_start, codegen.CharPtrType()), storage_size_);
}

}  // namespace codegen
}  // namespace peloton
