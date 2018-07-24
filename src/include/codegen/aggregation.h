//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregation.h
//
// Identification: src/include/codegen/aggregation.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "codegen/codegen.h"
#include "codegen/updateable_storage.h"
#include "codegen/value.h"
#include "planner/aggregate_plan.h"

namespace peloton {
namespace codegen {

/**
 * This class is responsible for handling the logic around performing
 * aggregations. Users first setup the aggregation (through Setup()) with all
 * the aggregates they wish calculate. Next, callers provided the initial values
 * of all the aggregates using a call to CreateInitialValues(). Each update to
 * the set of aggregates is made through AdvanceValues(), with updated values
 * for each aggregate. When done, a final call to FinalizeValues() is made to
 * collect all the final aggregate values.
 *
 * Note: the ordering of aggregates and values must be consistent with the
 *       ordering provided during Setup().
 */
class Aggregation {
 public:
  Aggregation();

  /**
   * Configure the aggregation to handle the aggregates of the provided format
   *
   * @param codegen The codegen instance
   * @param agg_terms A list of aggregate types
   * @param is_global
   */
  void Setup(CodeGen &codegen,
             const std::vector<planner::AggregatePlan::AggTerm> &agg_terms,
             bool is_global);

  /**
   * Create default initial values for all global aggregate components.
   *
   * @param codegen The codegen instance
   * @param aggs A pointer to where all aggregates are contiguously stored
   */
  void CreateInitialGlobalValues(CodeGen &codegen, llvm::Value *aggs) const;

  /**
   * Initialize a set of stored aggregates with the provided initial values
   *
   * @param codegen The codegen instance
   * @param aggs A pointer to where all aggregates are contiguously stored
   * @param initial_vals The initial values of each aggregate
   */
  void CreateInitialValues(
      CodeGen &codegen, llvm::Value *aggs,
      const std::vector<codegen::Value> &initial_vals) const;

  /**
   * Advance all aggregates stored contiguously in the provided storage space
   * using the delta values in the provided input vector
   *
   * @param codegen The codegen instance
   * @param aggs A pointer to where all aggregates are contiguously stored
   * @param next The list of values to use to update each positionally-aligned
   * aggregate
   */
  void AdvanceValues(CodeGen &codegen, llvm::Value *aggs,
                     const std::vector<codegen::Value> &next) const;

  /**
   * Advance the value of the distinct aggregate at the given position using
   * the provided value. It is assumed that the provided aggregates follow the
   * same format as this instance.
   *
   * @param codegen The codegen instance
   * @param aggs A pointer to a collection of aggregates
   * @param index The index of the aggregate the caller wishes to advance
   * @param val The value used to advance the aggregate
   */
  void AdvanceDistinctValue(CodeGen &codegen, llvm::Value *aggs, uint32_t index,
                            const codegen::Value &val) const;

  /**
   * Merge the (partial) aggregates provided in the two arguments and store the
   * result in the destination aggregate argument. It is assumed that the
   * aggregate layout is the same between the source and destination. This
   * requires that the same instance of Aggregation was used to build each
   * partial aggregate instance.
   *
   * @param codegen The codegen instance
   * @param dest_aggs A pointer to a collection of partially accumulated
   * aggregates.
   * @param src_aggs A pointer to a collection of partially accumulated
   * aggregates that will be merged into the destination.
   */
  void MergePartialAggregates(CodeGen &codegen, llvm::Value *dest_aggs,
                              llvm::Value *src_aggs) const;

  /**
   * Compute the final values of all the aggregates stored in the provided
   * storage space, inserting them into the provided output vector.
   *
   * @param codegen The codegen instance
   * @param aggs A pointer to where all aggregates are contiguously stored
   * @param[out] final_vals Vector where the final aggregates are stored.
   */
  void FinalizeValues(CodeGen &codegen, llvm::Value *aggs,
                      std::vector<codegen::Value> &final_vals) const;

  /**
   * Get the total number of bytes needed to store all aggregate values
   *
   * @return Total size, in bytes, of all aggregates
   */
  uint32_t GetAggregatesStorageSize() const {
    return storage_.GetStorageSize();
  }

  /**
   * Get the underlying LLVM format of this aggregate storage
   *
   * @return The LLVM type of this aggregate storage
   */
  llvm::Type *GetAggregateStorageType() const {
    return storage_.GetStorageType();
  }

  /**
   * Compute the type needed for a sum aggregate given the input attribute type
   *
   * @param input_type The type of values we're summing over
   * @return The target type to use for the sum
   */
  static type::Type SumType(const type::Type &input_type);

  /**
   * Compute the type needed for a division given the type of the numerator and
   * denominator to the division operation.
   *
   * @param numerator_type The SQL type of the numerator
   * @param denominator_type The SQL type of the denominator
   * @return The type to use for the output of the division
   */
  static type::Type DividingAggType(const type::Type &numerator_type,
                                    const type::Type &denominator_type);

  /**
   * This structure maps higher-level aggregates to their physical storage in
   * the underlying aggregate storage space.
   *
   * Some aggregates decompose into multiple components. For example, AVG()
   * aggregates decompose into a SUM() and COUNT() components that each have
   * an instance of this struct. For example, in the case of an AVG(), three
   * AggregateInfo structs are maintained, one for the sum, the count, and the
   * average. The source index of each of these structures will be the same, but
   * the storage index (i.e., where each component is stored) will differ. Since
   * the sum and count are internal, their internal flags will be set to ensure
   * we do not expose them externally.
   *
   * Storing the mapping from the physical position the aggregate is stored to
   * where the caller expects them allows us to rearrange positions without
   * the caller knowing or caring.
   */
  struct AggregateInfo {
    // The overall type of the aggregation
    const ExpressionType agg_type;

    // The type of this aggregate
    const type::Type type;

    // The expected output type
    const type::Type output_type;

    // The position in the original (ordered) list of aggregates that this
    // aggregate is stored
    const uint32_t source_idx;

    // Th position in the physical storage where the aggregate is stored
    const uint32_t storage_idx;

    // Is this aggregate purely internal?
    bool internal;

    // Is this aggregate distinct?
    bool distinct;

    // Is this aggregate derived?
    bool derived;

    AggregateInfo(const ExpressionType _agg_type, const type::Type &_type,
                  const type::Type &_output_type, const uint32_t _source_index,
                  const uint32_t _storage_index, const bool _internal,
                  const bool _distinct, const bool _derived)
        : agg_type(_agg_type),
          type(_type),
          output_type(_output_type),
          source_idx(_source_index),
          storage_idx(_storage_index),
          internal(_internal),
          distinct(_distinct),
          derived(_derived) {}
  };

 private:
  /// Common helper function to advance the value of a single aggregate
  void AdvanceValue(CodeGen &codegen, llvm::Value *space,
                    const AggregateInfo &agg_info, const codegen::Value &next,
                    UpdateableStorage::NullBitmap &null_bitmap) const;

 private:
  // Is this a global aggregation?
  bool is_global_;

  // The list of aggregations we handle
  std::vector<AggregateInfo> aggregate_infos_;

  // The storage format we use to store values
  UpdateableStorage storage_;
};

}  // namespace codegen
}  // namespace peloton