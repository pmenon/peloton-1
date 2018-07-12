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
/// Global Group By Translator
///
////////////////////////////////////////////////////////////////////////////////

class GlobalGroupByTranslator::IterateDistinctTable
    : public HashTable::IterateCallback {
 public:
  IterateDistinctTable(llvm::Value *aggregates, const Aggregation &aggregation,
                       uint32_t agg_pos)
      : agg_space_(aggregates), aggregation_(aggregation), agg_pos_(agg_pos) {}

  void ProcessEntry(CodeGen &codegen, const std::vector<codegen::Value> &key,
                    UNUSED_ATTRIBUTE llvm::Value *values) const override {
    const codegen::Value &distinct = key.back();
    aggregation_.AdvanceDistinctValue(codegen, agg_space_, agg_pos_, distinct);
  }

  llvm::Value *agg_space_;
  const Aggregation &aggregation_;
  uint32_t agg_pos_;
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

      // Track metadata for distinct aggregate
      distinct_agg_infos_.emplace_back(agg_idx, ht_id, /* TL table id */ 0,
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

void GlobalGroupByTranslator::DefineAuxiliaryFunctions() {}

void GlobalGroupByTranslator::Produce() const {
  // Initialize aggregation for global aggregation
  aggregation_.CreateInitialGlobalValues(GetCodeGen(),
                                         LoadStatePtr(mat_buffer_id_));

  // Let the child produce tuples that we'll aggregate
  GetCompilationContext().Produce(*GetPlan().GetChild(0));

  auto producer = [this](ConsumerContext &ctx) {
    CodeGen &codegen = GetCodeGen();
    auto *raw_vec =
        codegen.AllocateBuffer(codegen.Int32Type(), 1, "globalGbSelVector");
    Vector selection_vector{raw_vec, 1, codegen.Int32Type()};
    selection_vector.SetValue(codegen, codegen.Const32(0), codegen.Const32(0));

    // Create a row-batch of one row, place all the attributes into the row
    RowBatch batch{GetCompilationContext(), codegen.Const32(0),
                   codegen.Const32(1), selection_vector, false};

    // Deserialize the finalized aggregate attribute values from the buffer
    std::vector<codegen::Value> aggregate_vals;
    aggregation_.FinalizeValues(GetCodeGen(), LoadStatePtr(mat_buffer_id_),
                                aggregate_vals);

    // Collect accessors for each aggregate
    const auto &plan = GetPlanAs<planner::AggregatePlan>();

    std::vector<BufferAttributeAccess> buffer_accessors;
    const auto &agg_terms = plan.GetUniqueAggTerms();
    PELOTON_ASSERT(agg_terms.size() == aggregate_vals.size());
    for (size_t i = 0; i < agg_terms.size(); i++) {
      buffer_accessors.emplace_back(aggregate_vals, i);
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
  /*
   * Here, we collect values (derived from attributes or expressions) to update
   * the aggregates in the hash table. Some aggregate terms have no input
   * expressions; these are COUNT(*)'s. We synthesize a constant, non-NULL
   * BIGINT 1 value for those counts.
   */

  std::vector<codegen::Value> vals;

  auto &codegen = GetCodeGen();
  const auto &plan = GetPlanAs<planner::AggregatePlan>();
  for (const auto &agg_term : plan.GetUniqueAggTerms()) {
    if (agg_term.expression != nullptr) {
      vals.emplace_back(row.DeriveValue(codegen, *agg_term.expression));
    } else {
      PELOTON_ASSERT(agg_term.agg_type == ExpressionType::AGGREGATE_COUNT_STAR);
      type::Type count_star_type(type::Type(type::BigInt::Instance(), false));
      vals.emplace_back(count_star_type, codegen.Const64(1));
    }
  }

  /*
   * Advance each of the aggregates in the buffer with their provided values
   */

  aggregation_.AdvanceValues(GetCodeGen(), LoadStatePtr(mat_buffer_id_), vals);

  /*
   * Update all the distinct aggregate hash tables, too
   */

  bool parallel = ctx.GetPipelineContext()->IsParallel();

  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_agg_info.hash_table;

    llvm::Value *distinct_table_ptr =
        parallel
            ? ctx.GetPipelineContext()->LoadStatePtr(
                  codegen, distinct_agg_info.tl_hash_table_id)
            : LoadStatePtr(distinct_agg_info.hash_table_id);

    std::vector<codegen::Value> key = {vals[distinct_agg_info.agg_pos]};

    distinct_table.ProbeOrInsert(
        codegen, distinct_table_ptr, /* hash val */ nullptr, key,
        HashTable::InsertMode::Normal, nullptr, nullptr);
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
  if (pipeline_ctx.GetPipeline() != child_pipeline_ &&
      !pipeline_ctx.IsParallel()) {
    return;
  }
}

void GlobalGroupByTranslator::InitializePipelineState(
    PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() != child_pipeline_ &&
      !pipeline_ctx.IsParallel()) {
    return;
  }
}

void GlobalGroupByTranslator::FinishPipeline(PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() != child_pipeline_) {
    return;
  }

  CodeGen &codegen = GetCodeGen();

  llvm::Value *aggregates = LoadStatePtr(mat_buffer_id_);

  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_agg_info.hash_table;

    llvm::Value *distinct_ht_ptr =
        LoadStatePtr(distinct_agg_info.hash_table_id);

    IterateDistinctTable iter(aggregates, aggregation_,
                              distinct_agg_info.agg_pos);
    distinct_table.Iterate(codegen, distinct_ht_ptr, iter);
  }
}

void GlobalGroupByTranslator::TearDownPipelineState(
    PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() != child_pipeline_ &&
      !pipeline_ctx.IsParallel()) {
    return;
  }
}

}  // namespace codegen
}  // namespace peloton