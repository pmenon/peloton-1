//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_translator.cpp
//
// Identification: src/codegen/operator/hash_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/hash_translator.h"

#include "planner/hash_plan.h"
#include "codegen/proxy/hash_table_proxy.h"
#include "codegen/operator/projection_translator.h"
#include "codegen/lang/vectorized_loop.h"
#include "codegen/type/integer_type.h"
#include "common/logger.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// Consumer Insert
///
////////////////////////////////////////////////////////////////////////////////

/**
 * The callback used when we probe the hash table when aggregating, but do
 * not find an existing entry. It passes the row on to the parent.
 */
class HashTranslator::ConsumerInsert : public HashTable::InsertCallback {
 public:
  // Constructor
  ConsumerInsert(ConsumerContext &context, RowBatch::Row &row)
      : context_(context), row_(row) {}

  // StoreValue the initial values of the aggregates into the provided storage
  void StoreValue(UNUSED_ATTRIBUTE CodeGen &codegen,
                  UNUSED_ATTRIBUTE llvm::Value *data_space) const override {
    // It is the first time this key appears, so we just pass it along pipeline
    context_.Consume(row_);
  }

  llvm::Value *GetValueSize(CodeGen &codegen) const override {
    return codegen.Const32(0);
  }

 private:
  // ConsumerContext on which the consume will be called
  ConsumerContext &context_;

  // The row that will be given to the parent
  RowBatch::Row &row_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Hash Translator
///
////////////////////////////////////////////////////////////////////////////////

HashTranslator::HashTranslator(const planner::HashPlan &hash_plan,
                               CompilationContext &context, Pipeline &pipeline)
    : OperatorTranslator(hash_plan, context, pipeline) {
  // Distincts are serial (for now ...)
  pipeline.SetSerial();

  CodeGen &codegen = GetCodeGen();

  // Register the hash-table instance in the runtime state
  QueryState &query_state = context.GetQueryState();
  hash_table_id_ =
      query_state.RegisterState("hash", HashTableProxy::GetType(codegen));

  // Prepare the input operator
  context.Prepare(*hash_plan.GetChild(0), pipeline);

  // Prepare the hash keys
  std::vector<type::Type> key_type;
  for (const auto &hash_key : hash_plan.GetHashKeys()) {
    context.Prepare(*hash_key);
    key_type.push_back(hash_key->ResultType());
  }

  // Create the hash table. We don't need to save any values, so value size is 0
  hash_table_ = HashTable(codegen, key_type, 0);
}

// Initialize the hash table instance
void HashTranslator::InitializeQueryState() {
  hash_table_.Init(GetCodeGen(), GetExecutorContextPtr(),
                   LoadStatePtr(hash_table_id_));
}

// Produce!
void HashTranslator::Produce() const {
  // Let the child produce its tuples which we aggregate in our hash-table
  GetCompilationContext().Produce(*GetHashPlan().GetChild(0));
}

void HashTranslator::Consume(ConsumerContext &context,
                             RowBatch::Row &row) const {
  // Collect the keys we use to probe the hash table
  std::vector<codegen::Value> key;
  CollectHashKeys(row, key);

  llvm::Value *hash_table_ptr = LoadStatePtr(hash_table_id_);
  llvm::Value *hash = nullptr;

  // Insert into the hash table; if it's unique, also send along the pipeline
  ConsumerInsert insert(context, row);
  hash_table_.ProbeOrInsert(GetCodeGen(), hash_table_ptr, hash, key,
                            HashTable::InsertMode::Normal, nullptr, &insert);
}

void HashTranslator::TearDownQueryState() {
  hash_table_.Destroy(GetCodeGen(), LoadStatePtr(hash_table_id_));
}

void HashTranslator::CollectHashKeys(RowBatch::Row &row,
                                     std::vector<codegen::Value> &key) const {
  CodeGen &codegen = GetCodeGen();
  for (const auto &hash_key : GetHashPlan().GetHashKeys()) {
    key.push_back(row.DeriveValue(codegen, *hash_key));
  }
}

const planner::HashPlan &HashTranslator::GetHashPlan() const {
  return GetPlanAs<planner::HashPlan>();
}

}  // namespace codegen
}  // namespace peloton