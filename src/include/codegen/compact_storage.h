//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compact_storage.h
//
// Identification: src/include/codegen/compact_storage.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "codegen/codegen.h"
#include "codegen/value.h"
#include "codegen/type/type.h"

namespace peloton {
namespace codegen {

/**
 * This class enables the compact storage of a given set of types into a
 * contiguous storage space. To use this, call Setup(...) with the types of the
 * elements you'd like to store. The class can then be used to store and
 * retrieve values to and from a given storage space.
 *
 * TODO: We don't actually handle NULL values, so not really compact.
 */
class CompactStorage {
 public:
  CompactStorage();

  /**
   * Setup this storage to store the given types (in the specified order)
   *
   * @param codegen The codegen instance
   * @param types The types of the elements stored in this storage space
   * @return The final constructed structure type that storing all elements
   */
  llvm::Type *Setup(CodeGen &codegen, const std::vector<type::Type> &types);

  /**
   * Store all provided values into the provided storage space
   *
   * @param codegen The codegen instance
   * @param area_start A pointer to a contiguous memory space large enough to
   * store all provided values
   * @param vals The values to store
   * @return A memory pointer to the first byte after the last stored element
   */
  llvm::Value *StoreValues(CodeGen &codegen, llvm::Value *area_start,
                           const std::vector<codegen::Value> &vals) const;

  /**
   * Load all values stored at the given storage space into the provided vector
   *
   * @param codegen The codegen instance
   * @param area_start A pointer to a contiguous memory area where values with
   * this storage format are stored.
   * @param[out] vals An output vector containing all the elements stored in the
   * storage space.
   * @return A memory pointer to the first byte after the last stored element
   */
  llvm::Value *LoadValues(CodeGen &codegen, llvm::Value *area_start,
                          std::vector<codegen::Value> &vals) const;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Return the constructed LLVM structure type for this storage format
   *
   * @return The generated struct type
   */
  llvm::Type *GetStorageType() const { return storage_type_; }

  /**
   * Return the maximum number of bytes needed to store elements of the
   * configured size.
   *
   * @return The number of bytes.
   */
  uint32_t MaxStorageSize() const { return storage_size_; }

  /**
   * Return the number of elements configured in this storage format
   *
   * @return
   */
  uint32_t GetNumElements() const {
    return static_cast<uint32_t>(schema_.size());
  }

 private:
  friend class UpdateableStorage;
  class BitmapReader;
  class BitmapWriter;

  /**
   * This internal struct captures metadata about each "entry" in this storage
   * format is configured to work with.
   */
  struct EntryInfo {
    // The LLVM type of this entry
    llvm::Type *type;

    // The index in the underlying storage where this entry is stored.
    uint32_t physical_index;

    // The index in the externally visible schema this element belongs to. Or,
    // the index where the user expects to find this element.
    uint32_t logical_index;

    // Indicates whether this is the length component of a variable length entry
    bool is_length;

    // The size (in bytes_ this entry occupies
    uint64_t num_bytes;
  };

 private:
  // The SQL types we store
  std::vector<type::Type> schema_;

  // The schema of the storage
  std::vector<EntryInfo> storage_format_;

  // The constructed finalized type
  llvm::Type *storage_type_;

  // The size of the constructed finalized type
  uint32_t storage_size_;
};

}  // namespace codegen
}  // namespace peloton
