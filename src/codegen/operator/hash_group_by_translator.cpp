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
#include "codegen/hash.h"
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

/**
 * This class is used to merge partial aggregates stored in two different tables
 */
class HashGroupByTranslator::ParallelMergePartial
    : public HashTable::MergeCallback {
 public:
  explicit ParallelMergePartial(const Aggregation &aggregation)
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

/**
 * This class is the callback used to update a group's aggregates with a new
 * distinct value.
 */
class HashGroupByTranslator::MergeDistinctAgg_AdvanceInMain
    : public HashTable::ProbeCallback {
 public:
  MergeDistinctAgg_AdvanceInMain(const Aggregation &aggregation,
                                 uint32_t agg_pos, const Value &distinct_val)
      : aggregation_(aggregation),
        agg_pos_(agg_pos),
        distinct_val_(distinct_val) {}

  void ProcessEntry(CodeGen &codegen, llvm::Value *value) const override {
    aggregation_.AdvanceDistinctValue(codegen, value, agg_pos_, distinct_val_);
  }

 private:
  const Aggregation &aggregation_;
  const uint32_t agg_pos_;
  const codegen::Value &distinct_val_;
};

/**
 * This class is the callback used to iterate over a hash table storing distinct
 * values for distinct aggregations. For each such distinct entry, we merge this
 * value into the main aggregation table.
 */
class HashGroupByTranslator::MergeDistinctAgg_IterateDistinct
    : public HashTable::IterateCallback {
 public:
  MergeDistinctAgg_IterateDistinct(const HashTable &agg_table,
                                   llvm::Value *ht_ptr,
                                   const Aggregation &aggregation,
                                   const uint32_t agg_pos)
      : agg_table_(agg_table),
        ht_ptr_(ht_ptr),
        aggregation_(aggregation),
        agg_pos_(agg_pos) {}

  void ProcessEntry(CodeGen &codegen, UNUSED_ATTRIBUTE llvm::Value *entry_ptr,
                    const std::vector<codegen::Value> &key,
                    UNUSED_ATTRIBUTE llvm::Value *values) const override {
    // The group key
    std::vector<codegen::Value> group_key(key.begin(), key.end() - 1);

    // A (distinct) value from the hash table to merge into the main table
    const codegen::Value distinct_val = key.back();

    // Perform the advancement
    MergeDistinctAgg_AdvanceInMain merge_distinct(aggregation_, agg_pos_,
                                                  distinct_val);
    agg_table_.ProbeOrInsert(codegen, ht_ptr_, nullptr, group_key,
                             HashTable::InsertMode::Normal, &merge_distinct,
                             nullptr);
  }

 private:
  const HashTable &agg_table_;
  llvm::Value *ht_ptr_;
  const Aggregation &aggregation_;
  const uint32_t agg_pos_;
};

/**
 * This class is used to rehash all entries stored in distinct hash tables.
 */
class HashGroupByTranslator::MergeDistinctAgg_Rehash
    : public HashTable::IterateCallback {
 public:
  void ProcessEntry(CodeGen &codegen, llvm::Value *entry_ptr,
                    const std::vector<codegen::Value> &key,
                    UNUSED_ATTRIBUTE llvm::Value *values) const override {
    // The group key
    std::vector<codegen::Value> group_key(key.begin(), key.end() - 1);

    // Hash it
    llvm::Value *hash = Hash::HashValues(codegen, group_key);

    // Store new hash value
    codegen.Store(EntryProxy::hash, entry_ptr, hash);
  }
};

/**
 * This class is used to merge the contents of two hash tables storing distinct
 * aggregate values. In this scenario, merging becomes a no-op since one of the
 * tables already contains an instance of the distinct aggregate.
 */
class HashGroupByTranslator::ParallelMergeDistinct
    : public HashTable::MergeCallback {
 public:
  void MergeValues(UNUSED_ATTRIBUTE CodeGen &codegen,
                   UNUSED_ATTRIBUTE llvm::Value *table_values,
                   UNUSED_ATTRIBUTE llvm::Value *new_values) const override {}
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
      if (agg_term.agg_type == ExpressionType::AGGREGATE_MIN ||
          agg_term.agg_type == ExpressionType::AGGREGATE_MAX) {
        continue;
      }

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
    const HashTable &distinct_table = distinct_agg_info.hash_table;
    distinct_table.Init(codegen, exec_ctx_ptr,
                        LoadStatePtr(distinct_agg_info.hash_table_id));
  }
}

void HashGroupByTranslator::DefineAuxiliaryFunctions() {
  /*
   * No helper functions needed for serial pipelines
   */

  if (child_pipeline_.IsSerial()) {
    return;
  }

  /*
   * Parallel pipelines are a different story. We need to generate a function
   * that coalesces (potentially multiple) partitions into a single global hash
   * table.
   */

  CodeGen &codegen = GetCodeGen();
  QueryState &query_state = GetCompilationContext().GetQueryState();

  {
    std::vector<FunctionDeclaration::ArgumentInfo> arg_infos = {
        {"queryState", query_state.GetType()->getPointerTo()},
        {"hashTable", HashTableProxy::GetType(codegen)->getPointerTo()},
        {"partitionList",
         EntryProxy::GetType(codegen)->getPointerTo()->getPointerTo()},
        {"begin", codegen.Int64Type()},
        {"end", codegen.Int64Type()}};
    FunctionDeclaration decl(codegen.GetCodeContext(), "mergePartition",
                             FunctionDeclaration::Visibility::Internal,
                             codegen.VoidType(), arg_infos);
    FunctionBuilder merge_parts(codegen.GetCodeContext(), decl);
    {
      auto *table = merge_parts.GetArgumentByName("hashTable");
      auto *partition = merge_parts.GetArgumentByName("partitionList");
      auto *begin = merge_parts.GetArgumentByName("begin");
      auto *end = merge_parts.GetArgumentByName("end");

      // Do the merge
      bool allow_grow = true;
      ParallelMergePartial parallel_merge(aggregation_);
      hash_table_.MergePartitionRange(codegen, table, partition, begin, end,
                                      HashTable::LockMode::NoLock,
                                      parallel_merge, allow_grow);

      merge_parts.ReturnAndFinish();
    }

    // The merging function
    merging_func_ = merge_parts.GetFunction();
  }

  for (auto &distinct_agg_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_agg_info.hash_table;

    // First define the merge-and-rehash function
    {
      std::vector<FunctionDeclaration::ArgumentInfo> arg_infos = {
          {"queryState", query_state.GetType()->getPointerTo()},
          {"hashTable", HashTableProxy::GetType(codegen)->getPointerTo()},
          {"partitionList",
           EntryProxy::GetType(codegen)->getPointerTo()->getPointerTo()},
          {"begin", codegen.Int64Type()},
          {"end", codegen.Int64Type()}};
      FunctionDeclaration decl(codegen.GetCodeContext(),
                               "distinctMergeAndRehash",
                               FunctionDeclaration::Visibility::Internal,
                               codegen.VoidType(), arg_infos);
      FunctionBuilder initial_merge(codegen.GetCodeContext(), decl);
      {
        auto *table = initial_merge.GetArgumentByName("hashTable");
        auto *partition = initial_merge.GetArgumentByName("partitionList");
        auto *begin = initial_merge.GetArgumentByName("begin");
        auto *end = initial_merge.GetArgumentByName("end");

        // Merge
        bool allow_grow = true;
        ParallelMergeDistinct noop_merge;
        distinct_table.MergePartitionRange(codegen, table, partition, begin,
                                           end, HashTable::LockMode::NoLock,
                                           noop_merge, allow_grow);

        // Rehash
        MergeDistinctAgg_Rehash rehash;
        distinct_table.Iterate(codegen, table, rehash);

        initial_merge.ReturnAndFinish();
      }

      distinct_agg_info.initial_merge = initial_merge.GetFunction();
    }

    // Now, define the final merge function
    {
      std::vector<FunctionDeclaration::ArgumentInfo> arg_infos = {
          {"queryState", query_state.GetType()->getPointerTo()},
          {"distinctEntry", EntryProxy::GetType(codegen)->getPointerTo()},
          {"mainAggTable", HashTableProxy::GetType(codegen)->getPointerTo()}};
      FunctionDeclaration decl(codegen.GetCodeContext(), "distinctFinalMerge",
                               FunctionDeclaration::Visibility::Internal,
                               codegen.VoidType(), arg_infos);
      FunctionBuilder final_merge(codegen.GetCodeContext(), decl);
      {
        auto *entry = final_merge.GetArgumentByName("distinctEntry");
        auto *table = final_merge.GetArgumentByName("mainAggTable");

        std::vector<codegen::Value> full_key;
        distinct_agg_info.hash_table.KeysForEntry(codegen, entry, full_key);

        std::vector<codegen::Value> group_key(full_key.begin(),
                                              full_key.end() - 1);

        const codegen::Value &distinct_val = full_key.back();

        MergeDistinctAgg_AdvanceInMain merge(
            aggregation_, distinct_agg_info.agg_pos, distinct_val);

        hash_table_.ProbeOrInsert(
            codegen, table, codegen.Load(EntryProxy::hash, entry), group_key,
            HashTable::InsertMode::Normal, &merge, nullptr);

        final_merge.ReturnAndFinish();
      }

      distinct_agg_info.final_merge = final_merge.GetFunction();
    }
  }
}

void HashGroupByTranslator::Produce() const {
  // The plan node
  const auto &plan = GetPlanAs<planner::AggregatePlan>();

  /*
   * The first thing we do is let the child produce tuples which we aggregate
   * in our hash table. In parallel mode, this aggregation happens partially
   * across a set of thread-local tables.
   */

  GetCompilationContext().Produce(*plan.GetChild(0));

  /*
   * We've consumed all tuples from the child plan nodes. We now scan our hash
   * table and produce tuples which we send to our parent node.
   *
   * We use a generic producer function that simply scans over a hash table
   * instance provided as a function argument. It is agnostic to execution mode.
   */

  const auto producer = [this, &plan](ConsumerContext &ctx, llvm::Value *ht) {
    CodeGen &codegen = GetCodeGen();

    // The selection vector
    auto *i32_type = codegen.Int32Type();
    auto vec_size = Vector::kDefaultVectorSize.load();
    auto *raw_vec = codegen.AllocateBuffer(i32_type, vec_size, "gbPosList");
    Vector sel_vec(raw_vec, vec_size, i32_type);

    // Iterate
    ProduceResults produce_results(ctx, plan, aggregation_);
    hash_table_.VectorizedIterate(codegen, ht, sel_vec, produce_results);
  };

  Pipeline &pipeline = GetPipeline();

  if (pipeline.IsSerial()) {
    /*
     * If the pipeline is serial, all the heavy lifting is done.  We just need
     * to load the global hash table (stored in the query state) and scan it.
     */

    const auto serial_producer = [this, &producer](ConsumerContext &ctx) {
      producer(ctx, LoadStatePtr(hash_table_id_));
    };

    // Run serially
    pipeline.RunSerial(serial_producer);

  } else {
    /*
     * This is a parallel pipeline. At this point, we have a single primary
     * aggregate table that has all the data.  All we need to do is execute a
     * partitioned scan over its contents.  To do so, we setup a call to
     * util::HashTable::ExecutePartitionedScan().  We need to do a partitioned
     * scan because the main hash table was build using a partitioned process.
     */

    auto *dispatch_func =
        HashTableProxy::ExecutePartitionedScan.GetFunction(GetCodeGen());

    std::vector<llvm::Value *> dispatch_args = {LoadStatePtr(hash_table_id_)};

    std::vector<llvm::Type *> pipeline_arg_types = {
        HashTableProxy::GetType(GetCodeGen())->getPointerTo()};

    const auto parallel_producer = [this, &producer](
                                       ConsumerContext &ctx,
                                       const std::vector<llvm::Value *> &args) {
      PELOTON_ASSERT(args.size() == 1);
      producer(ctx, args[0]);
    };

    // Run in parallel!
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
   * Next, derive and collect values to update the aggregates in the main
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

  PipelineContext *pipeline_ctx = ctx.GetPipelineContext();

  llvm::Value *table_ptr = nullptr;
  HashTable::InsertMode mode;

  if (pipeline_ctx->IsParallel()) {
    mode = HashTable::InsertMode::Partitioned;
    table_ptr = pipeline_ctx->LoadStatePtr(codegen, tl_hash_table_id_);
  } else {
    mode = HashTable::InsertMode::Normal;
    table_ptr = LoadStatePtr(hash_table_id_);
  }

  /*
   * Update the main aggregation hash table!
   */

  ConsumerProbe probe(aggregation_, vals);
  ConsumerInsert insert(aggregation_, vals);
  hash_table_.ProbeOrInsert(codegen, table_ptr, nullptr, group_key, mode,
                            &probe, &insert);

  /*
   * Now, update all distinct aggregates' hash tables
   */

  for (const auto &distinct_info : distinct_agg_infos_) {
    const HashTable &distinct_hash_table = distinct_info.hash_table;

    // The key used to insert into the distinct table
    codegen::Value distinct_val =
        row.DeriveValue(codegen, *agg_terms[distinct_info.agg_pos].expression);

    std::vector<codegen::Value> distinct_key = group_key;
    distinct_key.push_back(distinct_val);

    // The pointer to the distinct table
    llvm::Value *ht_ptr = nullptr;
    if (pipeline_ctx->IsParallel()) {
      ht_ptr =
          pipeline_ctx->LoadStatePtr(codegen, distinct_info.tl_hash_table_id);
    } else {
      ht_ptr = LoadStatePtr(distinct_info.hash_table_id);
    }

    // Update. We don't need a probe or insert callback, hence the NULL args.
    distinct_hash_table.ProbeOrInsert(codegen, ht_ptr, nullptr, distinct_key,
                                      mode, nullptr, nullptr);
  }
}

void HashGroupByTranslator::TearDownQueryState() {
  // Destroy main aggregation table
  hash_table_.Destroy(GetCodeGen(), LoadStatePtr(hash_table_id_));

  // Destroy tables for distinct aggregates
  for (const auto &distinct_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_info.hash_table;
    distinct_table.Destroy(GetCodeGen(),
                           LoadStatePtr(distinct_info.hash_table_id));
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

  llvm::Type *ht_type = HashTableProxy::GetType(GetCodeGen());

  tl_hash_table_id_ = pipeline_ctx.RegisterState("tlGroupByHT", ht_type);

  /*
   * Additionally, each distinct aggregate will also get a thread-local hash
   * table. Register them here.
   */

  for (auto &distinct_info : distinct_agg_infos_) {
    distinct_info.tl_hash_table_id =
        pipeline_ctx.RegisterState("tlDistinctHT", ht_type);
  }
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
   * Initialize thread-local aggregation table
   */

  llvm::Value *tl_ht_ptr =
      pipeline_ctx.LoadStatePtr(codegen, tl_hash_table_id_);
  hash_table_.Init(codegen, exec_ctx_ptr, tl_ht_ptr);

  /*
   * Initialize any thread-local hash tables used for distinct aggregates
   */

  for (const auto &distinct_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_info.hash_table;

    llvm::Value *tl_distinct_ht_ptr =
        pipeline_ctx.LoadStatePtr(codegen, distinct_info.tl_hash_table_id);

    distinct_table.Init(codegen, exec_ctx_ptr, tl_distinct_ht_ptr);
  }
}

void HashGroupByTranslator::FinishPipeline(PipelineContext &pipeline_ctx) {
  /*
   * If the given pipeline context isn't for the child (consumer-side) pipeline,
   * we don't need to do anything special. Just exit.
   */
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
   * need to coalesce them into one. To enable this, we transfer all
   * thread-local partially accumulated aggregate data to the global aggregation
   * hash table stored in the query state object. This is facilitated with a
   * call to HashTable::TransferPartitions(...), so let's invoke it now.
   */

  llvm::Value *thread_states_ptr = GetThreadStatesPtr();

  hash_table_.TransferPartitions(
      codegen, LoadStatePtr(hash_table_id_), thread_states_ptr,
      pipeline_ctx.GetEntryOffset(codegen, tl_hash_table_id_), merging_func_);

  /*
   * The case for distinct aggregates is similar: their data, too, is spread
   * across thread-local hash tables. But, their merging process is a little
   * more complicated.
   *
   * As before, we transfer all thread-local distinct aggregate data to the
   * global distinct aggregate hash table to obtain a global view. This data is
   * spread across a set of overflow partitions that have to be merged to create
   * a partitioned hash table. We inject a special merging function for distinct
   * aggregates that not only removes duplicate entries (to support
   * distinction), but also rehashes all data using only the grouping key. Once
   * we finish rehashing, we repartition the hash table. Now, both the distinct
   * hash table and the main hash table are partitioned using the same key and
   * are merged together.
   */

  // The query state
  llvm::Value *query_state = codegen.GetState();

  // The main aggregate hash table
  llvm::Value *main_agg_table = LoadStatePtr(hash_table_id_);

  for (const auto &distinct_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_info.hash_table;

    // Load the global distinct table for this aggregate
    llvm::Value *distinct_ht_ptr = LoadStatePtr(distinct_info.hash_table_id);

    // First, transfer thread-local data to the global distinct table
    distinct_table.TransferPartitions(
        codegen, distinct_ht_ptr, thread_states_ptr,
        pipeline_ctx.GetEntryOffset(codegen, distinct_info.tl_hash_table_id),
        distinct_info.initial_merge);

    // Next, finish construction of hash table partitions
    distinct_table.FinishPartitions(codegen, distinct_ht_ptr, query_state);

    // Next, repartition all data since we've rehashed everything using just the
    // main grouping key (i.e., removing the distinct aggregate expression)
    distinct_table.Repartition(codegen, distinct_ht_ptr);

    // Now, merge it into the main aggregation hash table
    distinct_table.MergePartitions(codegen, distinct_ht_ptr, query_state,
                                   main_agg_table, distinct_info.final_merge);
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

  for (const auto &distinct_info : distinct_agg_infos_) {
    const HashTable &distinct_table = distinct_info.hash_table;

    llvm::Value *tl_distinct_ht_ptr =
        pipeline_ctx.LoadStatePtr(codegen, distinct_info.tl_hash_table_id);

    distinct_table.Destroy(codegen, tl_distinct_ht_ptr);
  }
}

void HashGroupByTranslator::MergeDistinctAggregates() const {
  CodeGen &codegen = GetCodeGen();

  llvm::Value *agg_ht_ptr = LoadStatePtr(hash_table_id_);

  for (const auto &distinct_info : distinct_agg_infos_) {
    // Load the pointer to this distinct aggregate's hash table
    llvm::Value *distinct_ht_ptr = LoadStatePtr(distinct_info.hash_table_id);

    // Load the hash table access class for this distinct aggregate
    const HashTable &hash_table = distinct_info.hash_table;

    // Do the merge
    MergeDistinctAgg_IterateDistinct merge_distinct(
        hash_table_, agg_ht_ptr, aggregation_, distinct_info.agg_pos);
    hash_table.Iterate(codegen, distinct_ht_ptr, merge_distinct);
  }
}

}  // namespace codegen
}  // namespace peloton