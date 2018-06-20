//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_translator.h
//
// Identification: src/include/codegen/operator/hash_translator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/aggregation.h"
#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/hash_table.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/updateable_storage.h"

namespace peloton {

namespace planner {
class HashPlan;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// The translator for a hash-based distinct operator.
//===----------------------------------------------------------------------===//
class HashTranslator : public OperatorTranslator {
 public:
  HashTranslator(const planner::HashPlan &hash_plan,
                 CompilationContext &context, Pipeline &pipeline);

  void InitializeQueryState() override;

  void DefineAuxiliaryFunctions() override {}

  void Produce() const override;

  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  void TearDownQueryState() override;

 private:
  void CollectHashKeys(RowBatch::Row &row,
                       std::vector<codegen::Value> &key) const;

  const planner::HashPlan &GetHashPlan() const;

 private:
  class ConsumerProbe;
  class ConsumerInsert;

  // The ID of the hash-table in the runtime state
  QueryState::Id hash_table_id_;

  // The hash table
  HashTable hash_table_;
};

}  // namespace codegen
}  // namespace peloton