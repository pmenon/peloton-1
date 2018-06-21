//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.cpp
//
// Identification: src/codegen/operator/hash_group_by_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/hash_group_by_translator.h"

#include "codegen/compilation_context.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/operator/projection_translator.h"
#include "codegen/proxy/hash_table_proxy.h"
#include "codegen/type/bigint_type.h"
#include "codegen/type/integer_type.h"

namespace peloton {
namespace codegen {

std::atomic<bool> HashGroupByTranslator::kUsePrefetch{false};

////////////////////////////////////////////////////////////////////////////////
///
/// Aggregate Finalizer
///
////////////////////////////////////////////////////////////////////////////////

class HashGroupByTranslator::AggregateFinalizer {
 public:
  AggregateFinalizer(const Aggregation &aggregation,
                     HashTable::HashTableAccess &hash_table_access)
      : aggregation_(aggregation),
        hash_table_access_(hash_table_access),
        finalized_(false) {}

  const std::vector<codegen::Value> &GetAggregates(CodeGen &codegen,
                                                   llvm::Value *index) {
    if (finalized_) {
      return final_aggregates_;
    }

    // Extract keys from bucket
    hash_table_access_.ExtractBucketKeys(codegen, index, final_aggregates_);

    // Extract aggregate values
    llvm::Value *data_area = hash_table_access_.BucketValue(codegen, index);
    aggregation_.FinalizeValues(codegen, data_area, final_aggregates_);

    finalized_ = true;

    return final_aggregates_;
  }

 private:
  // The aggregator
  const Aggregation &aggregation_;
  // The hash-table accessor
  HashTable::HashTableAccess &hash_table_access_;
  // Whether the aggregate has been finalized and the results
  bool finalized_;
  std::vector<codegen::Value> final_aggregates_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Aggregate Access
///
////////////////////////////////////////////////////////////////////////////////

class HashGroupByTranslator::AggregateAccess
    : public RowBatch::AttributeAccess {
 public:
  AggregateAccess(AggregateFinalizer &finalizer, uint32_t agg_index)
      : finalizer_(finalizer), agg_index_(agg_index) {}

  codegen::Value Access(CodeGen &codegen, RowBatch::Row &row) override {
    auto *pos = row.GetTID(codegen);
    const auto &final_agg_vals = finalizer_.GetAggregates(codegen, pos);
    return final_agg_vals[agg_index_];
  }

 private:
  // The associate finalizer
  AggregateFinalizer &finalizer_;
  // The index in the tuple's attributes
  uint32_t agg_index_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Consumer Probe Logic
///
////////////////////////////////////////////////////////////////////////////////

/**
 * This class is the callback issued when we find an existing group in the
 * aggregation hash table during table building. When called, we advance all the
 * stored aggregates.
 */
class HashGroupByTranslator::ConsumerProbe : public HashTable::ProbeCallback {
 public:
  ConsumerProbe(const Aggregation &aggregation,
                const std::vector<codegen::Value> &next_vals)
      : aggregation_(aggregation), next_vals_(next_vals) {}

  void ProcessEntry(CodeGen &codegen, llvm::Value *data_area) const override {
    aggregation_.AdvanceValues(codegen, data_area, next_vals_);
  }

 private:
  // The guy that handles the computation of the aggregates
  const Aggregation &aggregation_;
  // The next value to merge into the existing aggregates
  const std::vector<codegen::Value> &next_vals_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Consumer Insert Logic
///
////////////////////////////////////////////////////////////////////////////////

/**
 * This class is the callback issued when we do not find an existing group entry
 * in the aggregation hash table. When called, we insert a new group into the
 * hash table.
 */
class HashGroupByTranslator::ConsumerInsert : public HashTable::InsertCallback {
 public:
  ConsumerInsert(const Aggregation &aggregation,
                 const std::vector<codegen::Value> &initial_vals)
      : aggregation_(aggregation), initial_vals_(initial_vals) {}

  void StoreValue(CodeGen &codegen, llvm::Value *space) const override {
    aggregation_.CreateInitialValues(codegen, space, initial_vals_);
  }

  llvm::Value *GetValueSize(CodeGen &codegen) const override {
    return codegen.Const32(aggregation_.GetAggregatesStorageSize());
  }

 private:
  // The guy that handles the computation of the aggregates
  const Aggregation &aggregation_;
  // The list of initial values to use as aggregates
  const std::vector<codegen::Value> &initial_vals_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Produce Results Logic
///
////////////////////////////////////////////////////////////////////////////////

/**
 * This class serves as the callback when iterating over aggregation hash-table.
 */
class HashGroupByTranslator::ProduceResults
    : public HashTable::VectorizedIterateCallback {
 public:
  ProduceResults(ConsumerContext &ctx, const planner::AggregatePlan &plan,
                 const Aggregation &aggregation)
      : ctx_(ctx), plan_(plan), aggregation_(aggregation) {}

  void ProcessEntries(CodeGen &codegen, llvm::Value *start, llvm::Value *end,
                      Vector &selection_vector,
                      HashTable::HashTableAccess &access) const override;

 private:
  ConsumerContext &ctx_;
  const planner::AggregatePlan &plan_;
  const Aggregation &aggregation_;
};

void HashGroupByTranslator::ProduceResults::ProcessEntries(
    CodeGen &codegen, llvm::Value *start, llvm::Value *end,
    Vector &selection_vector, HashTable::HashTableAccess &access) const {
  // The row batch
  RowBatch batch(ctx_.GetCompilationContext(), start, end, selection_vector,
                 true);

  AggregateFinalizer finalizer(aggregation_, access);

  const auto &grouping_ais = plan_.GetGroupbyAIs();
  const auto &aggregates = plan_.GetUniqueAggTerms();

  std::vector<AggregateAccess> accessors;

  // Add accessors for each grouping key and aggregate value
  for (uint64_t i = 0; i < grouping_ais.size() + aggregates.size(); i++) {
    accessors.emplace_back(finalizer, i);
  }

  // Register attributes in the row batch
  for (uint64_t i = 0; i < grouping_ais.size(); i++) {
    batch.AddAttribute(grouping_ais[i], &accessors[i]);
  }
  for (uint64_t i = 0; i < aggregates.size(); i++) {
    auto &agg_term = aggregates[i];
    batch.AddAttribute(&agg_term.agg_ai, &accessors[i + grouping_ais.size()]);
  }

  std::vector<RowBatch::ExpressionAccess> derived_attribute_accessors;
  const auto *project_info = plan_.GetProjectInfo();
  if (project_info != nullptr) {
    ProjectionTranslator::AddNonTrivialAttributes(batch, *project_info,
                                                  derived_attribute_accessors);
  }

  // Row batch is set up, send it up
  auto *predicate = plan_.GetPredicate();
  if (predicate != nullptr) {
    // Iterate over the batch, performing a branching predicate check
    batch.Iterate(codegen, [&](RowBatch::Row &row) {
      codegen::Value valid_row = row.DeriveValue(codegen, *predicate);
      lang::If is_valid_row(codegen, valid_row);
      {
        // The row is valid, send along the pipeline
        ctx_.Consume(row);
      }
      is_valid_row.EndIf();
    });

  } else {
    // There isn't a predicate, just send the entire batch as-is
    ctx_.Consume(batch);
  }
}

////////////////////////////////////////////////////////////////////////////////
///
/// Parallel Merge
///
////////////////////////////////////////////////////////////////////////////////

class HashGroupByTranslator::ParallelMerge : public HashTable::MergeCallback {
 public:
  explicit ParallelMerge(const Aggregation &aggregation)
      : aggregation_(aggregation) {}

  void MergeValues(CodeGen &codegen, llvm::Value *table_values,
                   llvm::Value *new_values) const override {
    aggregation_.MergePartialAggregates(codegen, table_values, new_values);
  }

 private:
  const Aggregation &aggregation_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Merge Distinct
///
////////////////////////////////////////////////////////////////////////////////

class HashGroupByTranslator::IterateDistinctTable_MergeAggregates
    : public HashTable::IterateCallback {
 public:
  IterateDistinctTable_MergeAggregates(const HashTable &agg_table,
                                       llvm::Value *ht_ptr,
                                       const Aggregation &aggregation,
                                       uint32_t agg_pos)
      : agg_table_(agg_table),
        ht_ptr_(ht_ptr),
        aggregation_(aggregation),
        agg_pos_(agg_pos) {}

  void ProcessEntry(CodeGen &codegen, const std::vector<codegen::Value> &keys,
                    UNUSED_ATTRIBUTE llvm::Value *values) const override {
    class MergeDistinctValue : public HashTable::ProbeCallback {
     public:
      MergeDistinctValue(const Aggregation &_aggregation, uint32_t _agg_pos,
                         const Value &_distinct_val)
          : aggregation(_aggregation),
            agg_pos(_agg_pos),
            distinct_val(_distinct_val) {}

      void ProcessEntry(CodeGen &codegen, llvm::Value *value) const override {
        aggregation.MergeDistinct(codegen, value, agg_pos, distinct_val);
      }

     private:
      const Aggregation &aggregation;
      uint32_t agg_pos;
      const codegen::Value &distinct_val;
    };

    std::vector<codegen::Value> group_key(keys.begin(), keys.end() - 1);
    codegen::Value distinct_val = keys.back();

    MergeDistinctValue merge_distinct(aggregation_, agg_pos_, distinct_val);
    agg_table_.ProbeOrInsert(codegen, ht_ptr_, nullptr /* hash value */,
                             group_key, HashTable::InsertMode::Normal,
                             &merge_distinct, nullptr);
  }

 private:
  const HashTable &agg_table_;
  llvm::Value *ht_ptr_;
  const Aggregation &aggregation_;
  uint32_t agg_pos_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Hash Group By Translator Logic
///
////////////////////////////////////////////////////////////////////////////////

HashGroupByTranslator::HashGroupByTranslator(
    const planner::AggregatePlan &group_by, CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(group_by, context, pipeline),
      child_pipeline_(this, Pipeline::Parallelism::Flexible),
      merging_func_(nullptr) {
  // Prepare the input operator to this group by
  context.Prepare(*group_by.GetChild(0), child_pipeline_);

  /*
   * Currently, hash-based aggregations are parallelized only if the child
   * pipeline is also parallelized.
   */

  pipeline.MarkSource(this, child_pipeline_.IsParallel()
                                ? Pipeline::Parallelism::Parallel
                                : Pipeline::Parallelism::Serial);
  if (pipeline.IsSerial()) {
    child_pipeline_.SetSerial();
  }

  /*
   * If we should be pre-fetching into the hash-table, install a boundary in the
   * pipeline at the input into this translator to ensure it receives a vector
   * of input tuples
   */

  if (UsePrefetching()) {
    child_pipeline_.InstallStageBoundary(this);
  }

  // Register the hash-table instance in the runtime state
  CodeGen &codegen = GetCodeGen();
  QueryState &query_state = context.GetQueryState();
  hash_table_id_ =
      query_state.RegisterState("groupByHT", HashTableProxy::GetType(codegen));

  // Prepare the predicate if one exists
  if (group_by.GetPredicate() != nullptr) {
    context.Prepare(*group_by.GetPredicate());
  }

  // Prepare the grouping expressions
  std::vector<type::Type> key_type;
  for (const auto *grouping_ai : group_by.GetGroupbyAIs()) {
    key_type.push_back(grouping_ai->type);
  }

  /*
   * Prepare translators for all the aggregates. We also find all distinct
   * aggregates and arrange a hash table instance for them too.
   */

  const auto &aggregates = group_by.GetUniqueAggTerms();

  for (uint32_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
    const auto &agg_term = aggregates[agg_idx];

    if (agg_term.expression != nullptr) {
      context.Prepare(*agg_term.expression);
    }

    if (agg_term.distinct) {
      // Allocate a hash table
      QueryState::Id ht_id = query_state.RegisterState(
          "distinctHT", HashTableProxy::GetType(codegen));

      // The key type
      std::vector<type::Type> distinct_key = key_type;
      distinct_key.emplace_back(agg_term.expression->ResultType());

      // Track metadata for distinct aggregate
      distinct_agg_infos_.emplace_back(agg_idx, ht_id, 0 /* TL table id */,
                                       HashTable(codegen, distinct_key, 0));
    }
  }

  // Prepare the projection (if one exists)
  const auto *projection_info = group_by.GetProjectInfo();
  if (projection_info != nullptr) {
    ProjectionTranslator::PrepareProjection(context, *projection_info);
  }

  // Setup the aggregation logic for this group by
  aggregation_.Setup(codegen, aggregates, false /* is global */);

  // Create the hash table
  auto payload_size = aggregation_.GetAggregatesStorageSize();
  hash_table_ = HashTable(codegen, key_type, payload_size);
}

void HashGroupByTranslator::InitializeQueryState() {
  CodeGen &codegen = GetCodeGen();

  llvm::Value *exec_ctx_ptr = GetExecutorContextPtr();

  // Initialize the primary aggregation hash table
  hash_table_.Init(codegen, exec_ctx_ptr, LoadStatePtr(hash_table_id_));

  // Initialize tables for distinct aggregates
  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    hash_table_.Init(codegen, exec_ctx_ptr,
                     LoadStatePtr(distinct_agg_info.hash_table_id));
  }
}

void HashGroupByTranslator::DefineAuxiliaryFunctions() {
  if (child_pipeline_.IsSerial()) {
    return;
  }

  /*
   * The child pipeline is parallel. Hence, we need to generate a merging
   * function to coalesce multiple partitions into a single hash table.
   */

  auto &codegen = GetCodeGen();
  auto &query_state = GetCompilationContext().GetQueryState();

  std::vector<FunctionDeclaration::ArgumentInfo> arg_infos = {
      {"queryState", query_state.GetType()->getPointerTo()},
      {"hashTable", HashTableProxy::GetType(codegen)->getPointerTo()},
      {"partition",
       EntryProxy::GetType(codegen)->getPointerTo()->getPointerTo()}};
  FunctionDeclaration decl(codegen.GetCodeContext(), "mergePartition",
                           FunctionDeclaration::Visibility::Internal,
                           codegen.VoidType(), arg_infos);
  FunctionBuilder merge_parts(codegen.GetCodeContext(), decl);
  {
    auto *table = merge_parts.GetArgumentByName("hashTable");
    auto *partition = merge_parts.GetArgumentByName("partition");

    // Do the merge
    ParallelMerge parallel_merge(aggregation_);
    hash_table_.MergePartition(codegen, table, partition, parallel_merge);

    merge_parts.ReturnAndFinish();
  }

  // The merging function
  merging_func_ = merge_parts.GetFunction();
}

void HashGroupByTranslator::Produce() const {
  /*
   * The first thing we do is let the child produce tuples which we aggregate
   * in our hash table. In parallel mode, this aggregation happens partially
   * across a set of thread-local tables.
   */

  GetCompilationContext().Produce(*GetPlan().GetChild(0));

  /*
   * We've consumed all tuples from the child plan nodes. We now scan our hash
   * table and produce tuples which we send to our parent node.
   *
   * We use a generic producer function that simply scans over a hash table
   * instance provided as a function argument. It is agnostic to execution mode.
   */

  auto producer = [this](ConsumerContext &ctx, llvm::Value *ht) {
    CodeGen &codegen = GetCodeGen();

    // The selection vector
    auto *i32_type = codegen.Int32Type();
    auto vec_size = Vector::kDefaultVectorSize.load();
    auto *raw_vec = codegen.AllocateBuffer(i32_type, vec_size, "gbSelVec");
    Vector sel_vec(raw_vec, vec_size, i32_type);

    // Iterate
    const auto &plan = GetPlanAs<planner::AggregatePlan>();
    ProduceResults produce_results(ctx, plan, aggregation_);
    hash_table_.VectorizedIterate(codegen, ht, sel_vec, produce_results);
  };

  /* Handle parallel and serial pipelines differently */

  auto &pipeline = GetPipeline();

  if (pipeline.IsSerial()) {
    /*
     * This is a serial pipeline. So, we just need to load the global hash table
     * stored in the query state and scan it. To do so, we invoke the generic
     * hash table scanning function above using the global hash table.
     */

    auto serial_producer = [this, &producer](ConsumerContext &ctx) {
      producer(ctx, LoadStatePtr(hash_table_id_));
    };

    pipeline.RunSerial(serial_producer);

  } else {
    /*
     * At this point, we have one global hash table with potentially still
     * unmerged overflow partitions. Let's scan this partitioned table and
     * cooperatively and incrementally built a partitioned global hash table.
     */

    CodeGen &codegen = GetCodeGen();

    // Setup the call to util::HashTable::ExecutePartitionedScan()

    auto *dispatch_func =
        HashTableProxy::ExecutePartitionedScan.GetFunction(codegen);

    auto *hash_table = LoadStatePtr(hash_table_id_);
    auto *merge_func = codegen->CreatePointerCast(
        merging_func_,
        proxy::TypeBuilder<codegen::util::HashTable::MergingFunction>::GetType(
            codegen));

    std::vector<llvm::Value *> dispatch_args = {hash_table, merge_func};

    std::vector<llvm::Type *> pipeline_arg_types = {
        HashTableProxy::GetType(codegen)->getPointerTo()};

    auto parallel_producer = [this, &producer](
        ConsumerContext &ctx, std::vector<llvm::Value *> args) {
      PELOTON_ASSERT(args.size() == 1);
      producer(ctx, args[0]);
    };

    pipeline.RunParallel(dispatch_func, dispatch_args, pipeline_arg_types,
                         parallel_producer);
  }
}

void HashGroupByTranslator::Consume(ConsumerContext &context,
                                    RowBatch &batch) const {
  OperatorTranslator::Consume(context, batch);
#if 0
  if (!UsePrefetching()) {
    OperatorTranslator::Consume(context, batch);
    return;
  }

  // This aggregation uses prefetching

  CodeGen &codegen = GetCodeGen();

  // The vector holding the hash values for the group
  auto *raw_vec = codegen.AllocateBuffer(
      codegen.Int64Type(), OAHashTable::kDefaultGroupPrefetchSize, "pfVector");
  Vector hashes{raw_vec, OAHashTable::kDefaultGroupPrefetchSize,
                codegen.Int64Type()};

  auto group_prefetch = [&](
      RowBatch::VectorizedIterateCallback::IterationInstance &iter_instance) {
    llvm::Value *p = codegen.Const32(0);
    llvm::Value *end =
        codegen->CreateSub(iter_instance.end, iter_instance.start);

    // The first loop does hash computation and prefetching
    lang::Loop prefetch_loop{
        codegen, codegen->CreateICmpULT(p, end), {{"p", p}}};
    {
      p = prefetch_loop.GetLoopVar(0);
      RowBatch::Row row =
          batch.GetRowAt(codegen->CreateAdd(p, iter_instance.start));

      // Collect keys
      std::vector<codegen::Value> key;
      CollectHashKeys(row, key);

      // Hash the key and store in prefetch vector
      llvm::Value *hash_val = hash_table_.HashKey(codegen, key);

      // StoreValue hashed val in prefetch vector
      hashes.SetValue(codegen, p, hash_val);

      // Prefetch the actual hash table bucket
      hash_table_.PrefetchBucket(codegen, LoadStatePtr(hash_table_id_),
                                 hash_val, OAHashTable::PrefetchType::Read,
                                 OAHashTable::Locality::Medium);

      // End prefetch loop
      p = codegen->CreateAdd(p, codegen.Const32(1));
      prefetch_loop.LoopEnd(codegen->CreateICmpULT(p, end), {p});
    }

    p = codegen.Const32(0);
    std::vector<lang::Loop::LoopVariable> loop_vars = {
        {"p", p}, {"writeIdx", iter_instance.write_pos}};
    lang::Loop process_loop{codegen, codegen->CreateICmpULT(p, end), loop_vars};
    {
      p = process_loop.GetLoopVar(0);
      llvm::Value *write_pos = process_loop.GetLoopVar(1);

      llvm::Value *read_pos = codegen->CreateAdd(p, iter_instance.start);
      RowBatch::OutputTracker tracker{batch.GetSelectionVector(), write_pos};
      RowBatch::Row row = batch.GetRowAt(read_pos, &tracker);

      codegen::Value row_hash{type::Integer::Instance(),
                              hashes.GetValue(codegen, p)};
      row.RegisterAttributeValue(&OAHashTable::kHashAI, row_hash);

      // Consume row
      Consume(context, row);

      // End prefetch loop
      p = codegen->CreateAdd(p, codegen.Const32(1));
      process_loop.LoopEnd(codegen->CreateICmpULT(p, end),
                           {p, tracker.GetFinalOutputPos()});
    }

    std::vector<llvm::Value *> final_vals;
    process_loop.CollectFinalLoopVariables(final_vals);

    return final_vals[0];
  };

  batch.VectorizedIterate(codegen, OAHashTable::kDefaultGroupPrefetchSize,
                          group_prefetch);
#endif
}

void HashGroupByTranslator::Consume(ConsumerContext &ctx,
                                    RowBatch::Row &row) const {
  /*
   * First, collect the grouping key used to probe the main aggregate hash table
   */

  std::vector<codegen::Value> group_key;
  CollectGroupingKey(row, group_key);

  /*
   * Next, derived and collect values to update the aggregates in the main
   * aggregate hash table.
   */

  std::vector<codegen::Value> vals;

  CodeGen &codegen = GetCodeGen();

  const auto &agg_terms =
      GetPlanAs<planner::AggregatePlan>().GetUniqueAggTerms();
  for (const auto &agg_term : agg_terms) {
    if (agg_term.expression != nullptr) {
      vals.emplace_back(row.DeriveValue(codegen, *agg_term.expression));
    } else {
      /*
       * If an aggregate does not have an associated expression, it is most
       * likely a COUNT(*). We synthesize a constant, non-NULL BIGINT 1 value
       * for those counts.
       *
       */
      PELOTON_ASSERT(agg_term.agg_type == ExpressionType::AGGREGATE_COUNT_STAR);
      PELOTON_ASSERT(agg_term.agg_ai.type.type_id ==
                     ::peloton::type::TypeId::BIGINT);
      PELOTON_ASSERT(!agg_term.agg_ai.type.nullable);
      vals.emplace_back(agg_term.agg_ai.type, codegen.Const64(1));
    }
  }

  /*
   * We now have the grouping key and values needed to update the hash table.
   * But, the hash table we update depends on whether the aggregation is
   * parallelized or not. If in parallel, we update a thread-local table;
   * otherwise, we use a global hash table registered in the query state.
   *
   * Similarly, the insertion method we use for new groups depends on the
   * parallelization strategy chosen for the aggregation. If parallel, we insert
   * using a partitioned technique, and a "normal" insertion mode for serial
   * pipelines.
   */

  const bool parallel = ctx.GetPipeline().IsParallel();

  HashTable::InsertMode mode = parallel ? HashTable::InsertMode::Partitioned
                                        : HashTable::InsertMode::Normal;

  PipelineContext *pipeline_ctx = ctx.GetPipelineContext();
  llvm::Value *table_ptr =
      parallel ? pipeline_ctx->LoadStatePtr(codegen, tl_hash_table_id_)
               : LoadStatePtr(hash_table_id_);

  /*
   * Update the main aggregation hash table!
   */

  ConsumerProbe probe(aggregation_, vals);
  ConsumerInsert insert(aggregation_, vals);
  hash_table_.ProbeOrInsert(codegen, table_ptr, nullptr /* hash value */,
                            group_key, mode, &probe, &insert);

  /*
   * Now, update all hash tables for distinct aggregates, if any
   */

  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    // The key used to insert into the distinct table
    codegen::Value distinct_val = row.DeriveValue(
        codegen, *agg_terms[distinct_agg_info.agg_pos].expression);

    std::vector<codegen::Value> distinct_key = group_key;
    distinct_key.push_back(distinct_val);

    // The pointer to the distinct table
    llvm::Value *ht_ptr =
        LoadStatePtr(parallel ? distinct_agg_info.tl_hash_table_id
                              : distinct_agg_info.hash_table_id);

    // Insert
    hash_table_.ProbeOrInsert(codegen, ht_ptr, nullptr /* hash value */,
                              distinct_key, mode, nullptr, nullptr);
  }
}

void HashGroupByTranslator::TearDownQueryState() {
  // Destroy main aggregation table
  hash_table_.Destroy(GetCodeGen(), LoadStatePtr(hash_table_id_));

  // Destroy tables for distinct aggregates
  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    hash_table_.Destroy(GetCodeGen(),
                        LoadStatePtr(distinct_agg_info.hash_table_id));
  }
}

// Estimate the size of the dynamically constructed hash-table
uint64_t HashGroupByTranslator::EstimateHashTableSize() const {
  // TODO: Implement me
  return 0;
}

// Should this aggregation use prefetching
bool HashGroupByTranslator::UsePrefetching() const {
  // TODO: Implement me
  return kUsePrefetch;
}

void HashGroupByTranslator::CollectGroupingKey(
    RowBatch::Row &row, std::vector<codegen::Value> &key) const {
  CodeGen &codegen = GetCodeGen();
  const auto &plan = GetPlanAs<planner::AggregatePlan>();
  for (const auto *gb_ai : plan.GetGroupbyAIs()) {
    key.push_back(row.DeriveValue(codegen, gb_ai));
  }
}

void HashGroupByTranslator::RegisterPipelineState(
    PipelineContext &pipeline_ctx) {
  if (!pipeline_ctx.IsParallel() ||
      pipeline_ctx.GetPipeline() != child_pipeline_) {
    return;
  }

  /*
   * In parallel aggregation, we need thread-local hash tables. Allocate one
   * here in the pipeline context so each thread will get a hash table instance.
   */

  tl_hash_table_id_ = pipeline_ctx.RegisterState(
      "tlGroupBy", HashTableProxy::GetType(GetCodeGen()));
}

void HashGroupByTranslator::InitializePipelineState(
    PipelineContext &pipeline_ctx) {
  if (!pipeline_ctx.IsParallel() ||
      pipeline_ctx.GetPipeline() != child_pipeline_) {
    return;
  }

  CodeGen &codegen = GetCodeGen();

  llvm::Value *exec_ctx_ptr = GetExecutorContextPtr();

  /*
   * In parallel aggregation, each thread will have a thread-local hash table
   * instance defined in the thread state. We initialize it here.
   */

  llvm::Value *tl_ht_ptr =
      pipeline_ctx.LoadStatePtr(codegen, tl_hash_table_id_);
  hash_table_.Init(codegen, exec_ctx_ptr, tl_ht_ptr);

  /*
   * If there are distinct aggregates, each thread will also have a thread-local
   * hash table **FOR EACH** distinct aggregate. Initialize them all here, too.
   */

  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    const HashTable &hash_table = distinct_agg_info.hash_table;

    llvm::Value *tl_distinct_ht_ptr =
        pipeline_ctx.LoadStatePtr(codegen, distinct_agg_info.tl_hash_table_id);

    hash_table.Init(codegen, exec_ctx_ptr, tl_distinct_ht_ptr);
  }
}

void HashGroupByTranslator::FinishPipeline(PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() != child_pipeline_) {
    return;
  }

  /*
   * We've consumed all tuples from the child plan. If we have any distinct
   * aggregates, we need to merge those in before producing output tuples.
   */

  if (!pipeline_ctx.IsParallel()) {
    if (HasDistinctAggregates()) {
      MergeDistinctAggregates();
    }
    return;
  }

  CodeGen &codegen = GetCodeGen();

  /*
   * In parallel aggregation, after we're built thread-local hash tables, we
   * need to coalesce them into one. To do this, we transfer all thread-local
   * data to a global hash table (in a partitioned manner).
   */

  llvm::Value *global_ht_ptr = LoadStatePtr(hash_table_id_);
  llvm::Value *thread_states_ptr = GetThreadStatesPtr();
  uint32_t ht_offset = pipeline_ctx.GetEntryOffset(codegen, tl_hash_table_id_);

  codegen.Call(HashTableProxy::TransferPartitions,
               {global_ht_ptr, thread_states_ptr, codegen.Const32(ht_offset)});

  /*
   * Ditto for all distinct tables
   */

  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    llvm::Value *global_distinct_ht_ptr =
        LoadStatePtr(distinct_agg_info.tl_hash_table_id);

    uint32_t tl_distinct_ht_offset = pipeline_ctx.GetEntryOffset(
        codegen, distinct_agg_info.tl_hash_table_id);

    codegen.Call(HashTableProxy::TransferPartitions,
                 {global_distinct_ht_ptr, thread_states_ptr,
                  codegen.Const32(tl_distinct_ht_offset)});
  }
}

void HashGroupByTranslator::TearDownPipelineState(
    PipelineContext &pipeline_ctx) {
  if (!pipeline_ctx.IsParallel() ||
      pipeline_ctx.GetPipeline() != child_pipeline_) {
    return;
  }

  CodeGen &codegen = GetCodeGen();

  /*
   * In parallel aggregation, each thread will have a thread-local hash table
   * instance defined in the thread state. We destroy it here.
   */

  llvm::Value *tl_ht_ptr =
      pipeline_ctx.LoadStatePtr(codegen, tl_hash_table_id_);
  hash_table_.Destroy(codegen, tl_ht_ptr);

  /*
   * If there are distinct aggregates, each thread will also have a thread-local
   * hash table **FOR EACH** distinct aggregate. Destroy them all here, too.
   */

  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    const HashTable &hash_table = distinct_agg_info.hash_table;

    llvm::Value *tl_distinct_ht_ptr =
        pipeline_ctx.LoadStatePtr(codegen, distinct_agg_info.tl_hash_table_id);

    hash_table.Destroy(codegen, tl_distinct_ht_ptr);
  }
}

void HashGroupByTranslator::MergeDistinctAggregates() const {
  CodeGen &codegen = GetCodeGen();

  llvm::Value *agg_ht_ptr = LoadStatePtr(hash_table_id_);

  for (const auto &distinct_agg_info : distinct_agg_infos_) {
    // Load the pointer to this distinct aggregate's hash table
    llvm::Value *distinct_ht_ptr =
        LoadStatePtr(distinct_agg_info.hash_table_id);

    // Load the hash table access class for this distinct aggregate
    const HashTable &hash_table = distinct_agg_info.hash_table;

    // Do the merge
    IterateDistinctTable_MergeAggregates merge_distinct(
        hash_table_, agg_ht_ptr, aggregation_, distinct_agg_info.agg_pos);
    hash_table.Iterate(codegen, distinct_ht_ptr, merge_distinct);
  }
}

}  // namespace codegen
}  // namespace peloton