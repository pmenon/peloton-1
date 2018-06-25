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
   * @param space A pointer to where all aggregates are contiguously stored
   */
  void CreateInitialGlobalValues(CodeGen &codegen, llvm::Value *space) const;

  /**
   * Initialize a set of stored aggregates with the provided initial values
   *
   * @param codegen The codegen instance
   * @param space A pointer to where all aggregates are contiguously stored
   * @param initial_vals The initial values of each aggregate
   */
  void CreateInitialValues(
      CodeGen &codegen, llvm::Value *space,
      const std::vector<codegen::Value> &initial_vals) const;

  /**
   * Advance all aggregates stored contiguously in the provided storage space
   * using the delta values in the provided input vector
   *
   * @param codegen The codegen instance
   * @param space A pointer to where all aggregates are contiguously stored
   * @param next The list of values to use to update each positionally-aligned
   * aggregate
   */
  void AdvanceValues(CodeGen &codegen, llvm::Value *space,
                     const std::vector<codegen::Value> &next) const;

  /**
   *
   * @param codegen
   * @param space
   * @param index
   * @param val
   */
  void AdvanceDistinctValue(CodeGen &codegen, llvm::Value *space,
                            uint32_t index, const codegen::Value &val) const;

  /**
   *
   * @param codegen
   * @param curr_vals
   * @param new_vals
   */
  void MergePartialAggregates(CodeGen &codegen, llvm::Value *curr_vals,
                              llvm::Value *new_vals) const;

  /**
   * Compute the final values of all the aggregates stored in the provided
   * storage space, inserting them into the provided output vector.
   *
   * @param codegen The codegen instance
   * @param space A pointer to where all aggregates are contiguously stored
   * @param[out] final_vals Vector where the final aggregates are stored.
   */
  void FinalizeValues(CodeGen &codegen, llvm::Value *space,
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
   * Get the storage format of the aggregates this class is configured to handle
   *
   * @return
   */
  const UpdateableStorage &GetAggregateStorage() const { return storage_; }

  /**
   * This structure maps higher-level aggregates to their physical storage, and
   * to their hash tables if they are distinct.
   *
   * Some aggregates decompose into multiple components. For example, AVG()
   * aggregates decompose into a SUM() and COUNT(). Therefore, the storage
   * indexes are stored in an array. The array has fixed size of the maximum
   * number of components that a aggregation is decomposed to, so for now
   * only 2 for AVG. The aggregations have to know which component is
   * stored at which index.
   *
   * Storing the mapping from the physical position the aggregate is stored to
   * where the caller expects them allows us to rearrange positions without
   * the caller knowing or caring.
   */
  struct AggregateInfo {
    // The overall type of the aggregation
    const ExpressionType aggregate_type;

    // The position in the original (ordered) list of aggregates that this
    // aggregate is stored
    const uint32_t source_index;

    // Th position in the physical storage where the aggregate is stored
    const uint32_t storage_index;

    // Is this aggregate purely internal?
    bool internal;

    bool distinct;

    AggregateInfo(ExpressionType aggregate_type, uint32_t source_index,
                  uint32_t storage_index, bool _internal, bool _distinct);
  };

 private:
  void AdvanceSingleValue(CodeGen &codegen, llvm::Value *space,
                          const AggregateInfo &agg_info,
                          const codegen::Value &next,
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