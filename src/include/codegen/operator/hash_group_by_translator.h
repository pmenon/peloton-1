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
 * This is the primary translator for hash-based aggregations.
 */
class HashGroupByTranslator : public OperatorTranslator {
 public:
  // Global/configurable variable controlling whether hash aggregations prefetch
  static std::atomic<bool> kUsePrefetch;

  // Constructor
  HashGroupByTranslator(const planner::AggregatePlan &group_by,
                        CompilationContext &context, Pipeline &pipeline);

  // Codegen any initialization work for this operator
  void InitializeQueryState() override;

  // Define any helper functions this translator needs
  void DefineAuxiliaryFunctions() override;

  void RegisterPipelineState(PipelineContext &pipeline_ctx) override;
  void InitializePipelineState(PipelineContext &pipeline_ctx) override;
  void TearDownPipelineState(PipelineContext &pipeline_ctx) override;

  // The method that produces new tuples
  void Produce() const override;

  // The method that consumes tuples from child operators
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;
  void Consume(ConsumerContext &context, RowBatch &batch) const override;

  // Codegen any cleanup work for this translator
  void TearDownQueryState() override;

 private:
  // These helper classes we defined later
  class AggregateFinalizer;
  class AggregateAccess;
  class ConsumerProbe;
  class ConsumerInsert;
  class ProduceResults;
  class ParallelMerge;

 private:
  void CollectHashKeys(RowBatch::Row &row,
                       std::vector<codegen::Value> &key) const;

  // Estimate the size of the constructed hash table
  uint64_t EstimateHashTableSize() const;

  // Should this operator employ prefetching?
  bool UsePrefetching() const;

 private:
  // The pipeline forming all child operators of this aggregation
  Pipeline child_pipeline_;

  // The ID of the hash-table in the runtime state
  QueryState::Id hash_table_id_;
  PipelineContext::Id tl_hash_table_id_;

  // The hash table
  HashTable hash_table_;

  // The aggregation handler
  Aggregation aggregation_;

  // Hash table merging function, for partitioned aggregations
  llvm::Function *merging_func_;
};

}  // namespace codegen
}  // namespace peloton