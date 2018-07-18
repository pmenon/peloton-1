//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.h
//
// Identification: src/include/codegen/operator/hash_group_by_translator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/aggregation.h"
#include "codegen/hash_table.h"
#include "codegen/operator/operator_translator.h"

namespace peloton {

namespace planner {
class AggregatePlan;
}  // namespace planner

namespace codegen {

/**
 * This is the primary translator for all hash-based aggregations.
 */
class HashGroupByTranslator : public OperatorTranslator {
 public:
  // Global/configurable variable controlling whether hash aggregations prefetch
  static std::atomic<bool> kUsePrefetch;

  HashGroupByTranslator(const planner::AggregatePlan &group_by,
                        CompilationContext &context, Pipeline &pipeline);

  void InitializeQueryState() override;

  void DefineAuxiliaryFunctions() override;

  void RegisterPipelineState(PipelineContext &pipeline_ctx) override;
  void InitializePipelineState(PipelineContext &pipeline_ctx) override;
  void FinishPipeline(PipelineContext &context) override;
  void TearDownPipelineState(PipelineContext &pipeline_ctx) override;

  void Produce() const override;

  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;
  void Consume(ConsumerContext &context, RowBatch &batch) const override;

  void TearDownQueryState() override;

 private:
  // These are helper classes whose definitions aren't needed here, but are
  // defined later
  class AggregateFinalizer;
  class AggregateAccess;
  class ConsumerProbe;
  class ConsumerInsert;
  class ProduceResults;
  class ParallelMergePartial;
  class ParallelMergeDistinct;
  class MergeDistinctAgg_AdvanceInMain;
  class MergeDistinctAgg_IterateDistinct;
  class MergeDistinctAgg_Rehash;

 private:
  bool HasDistinctAggregates() const { return !distinct_agg_infos_.empty(); }

  void CollectGroupingKey(RowBatch::Row &row,
                          std::vector<codegen::Value> &key) const;

  // Estimate the size of the constructed hash table
  uint64_t EstimateHashTableSize() const;

  // Should this operator employ prefetching?
  bool UsePrefetching() const;

  //
  void MergeDistinctAggregates() const;

 private:
  // The pipeline forming all child operators of this aggregation
  Pipeline child_pipeline_;

  // The ID of the hash-table in the runtime state
  QueryState::Id hash_table_id_;
  PipelineContext::Id tl_hash_table_id_;

  // The main hash table
  HashTable hash_table_;

  // These vectors track the IDs of all hash tables used for distinct aggregates
  struct DistinctInfo {
    uint32_t agg_pos;
    QueryState::Id hash_table_id;
    PipelineContext::Id tl_hash_table_id;
    HashTable hash_table;
    llvm::Function *initial_merge;
    llvm::Function *final_merge;

    DistinctInfo(uint32_t _agg_pos, QueryState::Id _hash_table_id,
                 PipelineContext::Id _tl_hash_table_id, HashTable &&_hash_table)
        : agg_pos(_agg_pos),
          hash_table_id(_hash_table_id),
          tl_hash_table_id(_tl_hash_table_id),
          hash_table(std::move(_hash_table)),
          initial_merge(nullptr),
          final_merge(nullptr) {}
  };
  std::vector<DistinctInfo> distinct_agg_infos_;

  // The aggregation handler
  Aggregation aggregation_;

  // Hash table merging function, for partitioned aggregations
  llvm::Function *merging_func_;
};

}  // namespace codegen
}  // namespace peloton