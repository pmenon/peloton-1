//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// global_group_by_translator.h
//
// Identification: src/include/codegen/operator/global_group_by_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/aggregation.h"
#include "codegen/hash_table.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/pipeline.h"

namespace peloton {

namespace planner {
class AggregatePlan;
}  // namespace planner

namespace codegen {

/**
 * A global group-by aggregation is used when no grouping key is provided in the
 * query plan.
 */
class GlobalGroupByTranslator : public OperatorTranslator {
 public:
  GlobalGroupByTranslator(const planner::AggregatePlan &plan,
                          CompilationContext &context, Pipeline &pipeline);

  void InitializeQueryState() override;

  void DefineAuxiliaryFunctions() override {}

  void Produce() const override;

  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  void TearDownQueryState() override;

  void RegisterPipelineState(PipelineContext &pipeline_ctx) override;
  void InitializePipelineState(PipelineContext &pipeline_ctx) override;
  void FinishPipeline(PipelineContext &pipeline_ctx) override;
  void TearDownPipelineState(PipelineContext &pipeline_ctx) override;

 private:
  /// Helper classes defined later
  class BufferAttributeAccess;
  class ParallelMergeDistinct;
  class MergerDistinctAgg_IterateDistinct;

 private:
  // The pipeline the child operator of this aggregation belongs to
  Pipeline child_pipeline_;

  // The class responsible for handling the aggregation for all our aggregates
  Aggregation aggregation_;

  // The ID of our materialization buffer in the runtime state
  QueryState::Id mat_buffer_id_;
  PipelineContext::Id tl_mat_buffer_id_;

  // These vectors track the IDs of all hash tables used for distinct aggregates
  struct DistinctInfo {
    uint32_t agg_pos;
    QueryState::Id hash_table_id;
    PipelineContext::Id tl_hash_table_id;
    HashTable hash_table;

    DistinctInfo(uint32_t _agg_pos, QueryState::Id _hash_table_id,
                 PipelineContext::Id _tl_hash_table_id, HashTable &&_hash_table)
        : agg_pos(_agg_pos),
          hash_table_id(_hash_table_id),
          tl_hash_table_id(_tl_hash_table_id),
          hash_table(_hash_table) {}
  };

  std::vector<DistinctInfo> distinct_agg_infos_;
};

}  // namespace codegen
}  // namespace peloton