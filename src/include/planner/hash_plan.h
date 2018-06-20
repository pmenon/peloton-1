//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_plan.h
//
// Identification: src/include/planner/hash_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_plan.h"
#include "expression/abstract_expression.h"
#include "common/internal_types.h"

namespace peloton {

namespace expression {
class Parameter;
}  // namespace expression

namespace planner {

class HashPlan : public AbstractPlan {
 public:
  typedef const expression::AbstractExpression HashKeyType;
  typedef std::unique_ptr<HashKeyType> HashKeyPtrType;

  HashPlan(std::vector<HashKeyPtrType> &&hashkeys)
      : hash_keys_(std::move(hashkeys)) {}

  void PerformBinding(BindingContext &binding_context) override;

  void GetOutputColumns(std::vector<oid_t> &columns) const override;

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HASH; }

  const std::string GetInfo() const override { return "HashPlan"; }

  const std::vector<HashKeyPtrType> &GetHashKeys() const {
    return this->hash_keys_;
  }

  std::unique_ptr<AbstractPlan> Copy() const override;

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;

  void VisitParameters(codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

 private:
  std::vector<HashKeyPtrType> hash_keys_;

 private:
  DISALLOW_COPY_AND_MOVE(HashPlan);
};

}  // namespace planner
}  // namespace peloton
