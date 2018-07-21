//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// global_group_by_translator.cpp
//
// Identification: src/codegen/operator/global_group_by_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/global_group_by_translator.h"

#include "codegen/compilation_context.h"
#include "codegen/proxy/hash_table_proxy.h"
#include "codegen/type/bigint_type.h"
#include "codegen/vector.h"
#include "common/logger.h"
#include "planner/aggregate_plan.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// Buffer Attribute Accessor
///
////////////////////////////////////////////////////////////////////////////////

/**
 * An accessor into a single tuple stored in buffered state
 */
class GlobalGroupByTranslator::BufferAttributeAccess
    : public RowBatch::AttributeAccess {
 public:
  // Constructor
  BufferAttributeAccess(const std::vector<codegen::Value> &aggregate_vals,
                        uint32_t agg_index)
      : aggregate_vals_(aggregate_vals), agg_index_(agg_index) {}

  Value Access(CodeGen &, RowBatch::Row &) override {
    return aggregate_vals_[agg_index_];
  }

 private:
  // All the vals
  const std::vector<codegen::Value> &aggregate_vals_;

  // The value this accessor is for
  uint32_t agg_index_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Distinct logic
///
////////////////////////////////////////////////////////////////////////////////

class GlobalGroupByTranslator::MergerDistinctAgg_IterateDistinct
    : public HashTable::IterateCallback {
 public:
  MergerDistinctAgg_IterateDistinct(llvm::Value *aggregates,
                                    const Aggregation &aggregation,
                                    uint32_t agg_pos)
      : agg_space_(aggregates), aggregation_(aggregation), agg_pos_(agg_pos) {}

  void ProcessEntry(CodeGen &codegen, UNUSED_ATTRIBUTE llvm::Value *entry_ptr,
                    const std::vector<codegen::Value> &key,
                    UNUSED_ATTRIBUTE llvm::Value *values) const override {
    const codegen::Value &distinct = key.back();
    aggregation_.AdvanceDistinctValue(codegen, agg_space_, agg_pos_, distinct);
  }

  llvm::Value *agg_space_;
  const Aggregation &aggregation_;
  uint32_t agg_pos_;
};

class GlobalGroupByTranslator::ParallelMergeDistinct
    : public HashTable::MergeCallback {
 public:
  void MergeValues(UNUSED_ATTRIBUTE CodeGen &codegen,
                   UNUSED_ATTRIBUTE llvm::Value *table_values,
                   UNUSED_ATTRIBUTE llvm::Value *new_values) const override {}
};

////////////////////////////////////////////////////////////////////////////////
///
/// Global Group By Translator
///
////////////////////////////////////////////////////////////////////////////////

GlobalGroupByTranslator::GlobalGroupByTranslator(
    const planner::AggregatePlan &plan, CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(plan, context, pipeline),
      child_pipeline_(this, Pipeline::Parallelism::Flexible) {
  // Global aggregations produce a single tuple. No need to be parallel.
  pipeline.MarkSource(this, Pipeline::Parallelism::Serial);

  // Prepare the child in the new child pipeline
  context.Prepare(*plan.GetChild(0), child_pipeline_);

  CodeGen &codegen = context.GetCodeGen();

  // Allocate state in the function argument for our materialization buffer
  QueryState &query_state = context.GetQueryState();

  // Prepare all the aggregating expressions
  const auto &aggregates = plan.GetUniqueAggTerms();
  for (uint32_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
    const auto &agg_term = aggregates[agg_idx];

    if (agg_term.expression != nullptr) {
      context.Prepare(*agg_term.expression);
    }

    if (agg_term.distinct) {
      // MIN/MAX don't need distinct aggregate tables
      if (agg_term.agg_type == ExpressionType::AGGREGATE_MIN ||
          agg_term.agg_type == ExpressionType::AGGREGATE_MAX) {
        continue;
      }

      // Allocate a hash table
      QueryState::Id ht_id = query_state.RegisterState(
          "distinctHT", HashTableProxy::GetType(codegen));

      // The key type
      std::vector<type::Type> distinct_key = {
          agg_term.expression->ResultType()};

      /*
       * Track metadata for distinct aggregate. We initially set the
       * thread-local distinct hash table ID to 0. It will be filled in later
       * when we initialize the child pipeline. We also use a 0 value size
       * because we don't store actual values in the table. We only need the
       * key.
       */

      distinct_agg_infos_.emplace_back(agg_idx, ht_id, 0,
                                       HashTable(codegen, distinct_key, 0));
    }
  }

  // Setup the aggregation handler with the terms we use for aggregation
  aggregation_.Setup(codegen, aggregates, true);

  // Create the materialization buffer where we aggregate things
  llvm::Type *agg_storage_type = aggregation_.GetAggregateStorageType();
  mat_buffer_id_ = query_state.RegisterState("aggBuf", agg_storage_type);
}

void GlobalGroupByTranslator::InitializeQueryState() {
  CodeGen &codegen = GetCodeGen();

  llvm::Value *exec_ctx_ptr = GetExecutorContextPtr();

  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_agg_info.hash_table;

    llvm::Value *distinct_table_ptr =
        LoadStatePtr(distinct_agg_info.hash_table_id);

    distinct_table.Init(codegen, exec_ctx_ptr, distinct_table_ptr);
  }
}

void GlobalGroupByTranslator::Produce() const {
  // Initialize aggregation for global aggregation
  llvm::Value *agg_vals = LoadStatePtr(mat_buffer_id_);
  aggregation_.CreateInitialGlobalValues(GetCodeGen(), agg_vals);

  // Let the child produce tuples that we'll aggregate
  GetCompilationContext().Produce(*GetPlan().GetChild(0));

  /*
   * Aggregation is now complete. We just scan the results and send to parent.
   */

  const auto producer = [this](ConsumerContext &ctx) {
    CodeGen &codegen = GetCodeGen();
    auto *raw =
        codegen.AllocateBuffer(codegen.Int32Type(), 1, "globalGbPosList");
    Vector selection_vector(raw, 1, codegen.Int32Type());
    selection_vector.SetValue(codegen, codegen.Const32(0), codegen.Const32(0));

    // Create a row-batch of one row, place all the attributes into the row
    RowBatch batch(GetCompilationContext(), codegen.Const32(0),
                   codegen.Const32(1), selection_vector, false);

    // Deserialize the finalized aggregate attribute values from the buffer
    std::vector<codegen::Value> final_agg_vals;
    aggregation_.FinalizeValues(codegen, LoadStatePtr(mat_buffer_id_),
                                final_agg_vals);

    // Collect accessors for each aggregate
    const auto &plan = GetPlanAs<planner::AggregatePlan>();

    std::vector<BufferAttributeAccess> buffer_accessors;
    const auto &agg_terms = plan.GetUniqueAggTerms();
    PELOTON_ASSERT(agg_terms.size() == final_agg_vals.size());
    for (size_t i = 0; i < agg_terms.size(); i++) {
      buffer_accessors.emplace_back(final_agg_vals, i);
    }
    for (size_t i = 0; i < agg_terms.size(); i++) {
      batch.AddAttribute(&agg_terms[i].agg_ai, &buffer_accessors[i]);
    }

    // Create a new consumer context, send (single row) batch to parent
    ctx.Consume(batch);
  };

  // Run the last pipeline serially
  GetPipeline().RunSerial(producer);
}

void GlobalGroupByTranslator::Consume(ConsumerContext &ctx,
                                      RowBatch::Row &row) const {
  CodeGen &codegen = GetCodeGen();

  const auto &plan = GetPlanAs<planner::AggregatePlan>();

  /*
   * First collect input values, derived from attributes or expressions, used to
   * update the aggregates. Some aggregate terms have no input expressions;
   * these are COUNT(*)'s. We synthesize a constant, non-NULL BIGINT 1 value
   * for those counts.
   */

  std::vector<codegen::Value> vals;

  for (const auto &agg_term : plan.GetUniqueAggTerms()) {
    if (agg_term.expression != nullptr) {
      vals.emplace_back(row.DeriveValue(codegen, *agg_term.expression));
    } else {
      PELOTON_ASSERT(agg_term.agg_type == ExpressionType::AGGREGATE_COUNT_STAR);
      type::Type count_star_type(type::Type(type::BigInt::Instance(), false));
      vals.emplace_back(count_star_type, codegen.Const64(1));
    }
  }

  bool parallel = ctx.GetPipelineContext()->IsParallel();

  /*
   * Advance each of the aggregates in the buffer with their provided values.
   * The buffer we aggregate into depends on whether we're executing in parallel
   * or serially. If in parallel, we use the registered thread-local buffer;
   * otherwise, we use the global buffer registered in the query state.
   */

  llvm::Value *agg_buf = nullptr;

  if (parallel) {
    agg_buf =
        ctx.GetPipelineContext()->LoadStatePtr(codegen, tl_mat_buffer_id_);
  } else {
    agg_buf = LoadStatePtr(mat_buffer_id_);
  }

  aggregation_.AdvanceValues(GetCodeGen(), agg_buf, vals);

  /*
   * Now, we update all the distinct aggregate hash tables. Similarly, we need
   * to choose which distinct hash table to update depending on whether the
   * execution is parallel or serial.
   */

  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_agg_info.hash_table;

    llvm::Value *distinct_table_ptr = nullptr;
    if (parallel) {
      distinct_table_ptr = ctx.GetPipelineContext()->LoadStatePtr(
          codegen, distinct_agg_info.tl_hash_table_id);
    } else {
      distinct_table_ptr = LoadStatePtr(distinct_agg_info.hash_table_id);
    }

    std::vector<codegen::Value> key = {vals[distinct_agg_info.agg_pos]};

    distinct_table.ProbeOrInsert(codegen, distinct_table_ptr, nullptr, key,
                                 HashTable::InsertMode::Normal, nullptr,
                                 nullptr);
  }
}
void GlobalGroupByTranslator::TearDownQueryState() {
  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_agg_info.hash_table;

    llvm::Value *distinct_table_ptr =
        LoadStatePtr(distinct_agg_info.hash_table_id);

    distinct_table.Destroy(GetCodeGen(), distinct_table_ptr);
  }
}

void GlobalGroupByTranslator::RegisterPipelineState(
    PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() != child_pipeline_ ||
      !pipeline_ctx.IsParallel()) {
    return;
  }

  /*
   * In parallel mode, each thread has its own aggregate materialization buffer
   * that accumulates partial aggregates. Register them here.
   */

  tl_mat_buffer_id_ = pipeline_ctx.RegisterState(
      "tlAggBuf", aggregation_.GetAggregateStorageType());

  /*
   * Additionally, each distinct aggregate will also get a thread-local hash
   * table. Register them here.
   */

  for (auto &distinct_info : distinct_agg_infos_) {
    distinct_info.tl_hash_table_id = pipeline_ctx.RegisterState(
        "tlDistinctHT", HashTableProxy::GetType(GetCodeGen()));
  }
}

void GlobalGroupByTranslator::InitializePipelineState(
    PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() != child_pipeline_ ||
      !pipeline_ctx.IsParallel()) {
    return;
  }

  CodeGen &codegen = GetCodeGen();

  /*
   * Initialize the thread-local aggregation buffer
   */

  llvm::Value *tl_aggs = pipeline_ctx.LoadStatePtr(codegen, tl_mat_buffer_id_);
  aggregation_.CreateInitialGlobalValues(codegen, tl_aggs);

  /*
   * Initialize each thread-local hash table used for distinct aggregates.
   */

  llvm::Value *exec_ctx_ptr = GetExecutorContextPtr();

  for (auto &distinct_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_info.hash_table;

    llvm::Value *tl_distinct_ht_ptr =
        pipeline_ctx.LoadStatePtr(codegen, distinct_info.tl_hash_table_id);

    distinct_table.Init(codegen, exec_ctx_ptr, tl_distinct_ht_ptr);
  }
}

void GlobalGroupByTranslator::FinishPipeline(PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() != child_pipeline_) {
    return;
  }

  /*
   * Load the main aggregate buffer here since we'll use it throughout the rest
   * of this function.
   */

  llvm::Value *aggs = LoadStatePtr(mat_buffer_id_);

  /*
   * In parallel mode, we need to coalesce partially aggregated thread-local
   * data into a global hash table. Ditto for distinct aggregates. So let's do
   * that below.
   */

  if (pipeline_ctx.IsParallel()) {
    /*
     * First, merge in the partially accumulate aggregates directly into the
     * global aggregate materialization buffer. This is done serially since we
     * expect few thread-local aggregates (e.g., # partial aggregate buffers
     * will be O(T) where T is the number of execution threads), and the merging
     * process requires a few arithmetic computations per aggregate.
     */


    {
      PipelineContext::LoopOverStates merge_partial(pipeline_ctx);
      merge_partial.Do(
          [this, &pipeline_ctx, aggs](UNUSED_ATTRIBUTE llvm::Value *state) {
            llvm::Value *partial_aggs =
                pipeline_ctx.LoadStatePtr(GetCodeGen(), tl_mat_buffer_id_);
            aggregation_.MergePartialAggregates(GetCodeGen(),
                                                aggs,
                                                partial_aggs);
          });
    }

    /*
     * Next, we transfer all thread-local distinct table data into the global
     * distinct hash table for each distinct aggregate.
     */
    {
      for (const auto &distinct_info : distinct_agg_infos_) {
        PipelineContext::LoopOverStates merge_distinct(pipeline_ctx);
        merge_distinct.DoParallel([this, &pipeline_ctx, &distinct_info](
            UNUSED_ATTRIBUTE llvm::Value *state) {
          const HashTable &table = distinct_info.hash_table;

          llvm::Value *main_ht = LoadStatePtr(distinct_info.hash_table_id);

          llvm::Value *tl_ht = pipeline_ctx.LoadStatePtr(
              GetCodeGen(), distinct_info.tl_hash_table_id);

          // Merge contents
          bool multi_threaded = true;
          ParallelMergeDistinct noop_merge;
          table.Merge(GetCodeGen(), main_ht, tl_ht, noop_merge, multi_threaded);
        });
      }
    }
  }

  /*
   * At this point, the main aggregate table has finalized values for all
   * non-distinct aggregates. What's left is to merge in all data from each of
   * the distinct hash tables that we've built, too.
   */

  for (const auto &distinct_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_info.hash_table;

    llvm::Value *distinct_ht_ptr = LoadStatePtr(distinct_info.hash_table_id);

    MergerDistinctAgg_IterateDistinct iter(aggs, aggregation_,
                                           distinct_info.agg_pos);
    distinct_table.Iterate(GetCodeGen(), distinct_ht_ptr, iter);
  }
}

void GlobalGroupByTranslator::TearDownPipelineState(
    PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() != child_pipeline_ ||
      !pipeline_ctx.IsParallel()) {
    return;
  }

  /*
   * Destroy each thread-local hash table used for distinct aggregates.
   */

  CodeGen &codegen = GetCodeGen();

  for (auto &distinct_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_info.hash_table;

    llvm::Value *tl_distinct_ht_ptr =
        pipeline_ctx.LoadStatePtr(codegen, distinct_info.tl_hash_table_id);

    distinct_table.Destroy(codegen, tl_distinct_ht_ptr);
  }
}

}  // namespace codegen
}  // namespace peloton