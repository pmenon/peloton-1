//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_plan.h
//
// Identification: src/include/planner/aggregate_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <numeric>

#include "common/internal_types.h"
#include "planner/abstract_plan.h"
#include "planner/project_info.h"

namespace peloton {

namespace expression {
class AbstractExpression;
class Parameter;
}  // namespace expression

namespace planner {

class AggregatePlan : public AbstractPlan {
 public:
  /**
   * A single aggregate in the overall plan. For example, this may represent a
   * single AVG(a) on a column 'a'.
   */
  class AggTerm {
   public:
    ExpressionType agg_type;
    std::unique_ptr<expression::AbstractExpression> expression;
    bool distinct;

    // The attribute information and ID for this aggregate
    AttributeInfo agg_ai;

    AggTerm(ExpressionType et,
            std::unique_ptr<expression::AbstractExpression> &&expr,
            bool distinct = false);

    void PerformBinding(bool is_global, BindingContext &binding_context);

    hash_t Hash() const;

    bool operator==(const AggTerm &rhs) const;

    AggTerm Copy() const;
  };

  AggregatePlan(
      std::unique_ptr<const planner::ProjectInfo> &&project_info,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::vector<AggTerm> &&unique_agg_terms,
      std::vector<oid_t> &&groupby_col_ids,
      std::shared_ptr<const catalog::Schema> &output_schema,
      AggregateType aggregate_strategy)
      : project_info_(std::move(project_info)),
        predicate_(std::move(predicate)),
        unique_agg_terms_(std::move(unique_agg_terms)),
        groupby_col_ids_(std::move(groupby_col_ids)),
        output_schema_(output_schema),
        agg_strategy_(aggregate_strategy) {}

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Read-only Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  bool IsGlobal() const { return GetGroupbyColIds().empty(); }

  const std::vector<oid_t> &GetGroupbyColIds() const {
    return groupby_col_ids_;
  }

  const std::vector<const AttributeInfo *> &GetGroupbyAIs() const {
    return groupby_ais_;
  }

  const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  const std::vector<AggTerm> &GetUniqueAggTerms() const {
    return unique_agg_terms_;
  }

  const catalog::Schema *GetOutputSchema() const {
    return output_schema_.get();
  }

  AggregateType GetAggregateStrategy() const { return agg_strategy_; }

  inline PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::AGGREGATE_V2;
  }

  void GetOutputColumns(std::vector<oid_t> &columns) const override {
    columns.resize(GetOutputSchema()->GetColumnCount());
    std::iota(columns.begin(), columns.end(), 0);
  }

  const std::string GetInfo() const override {
    return "AggregatePlan(Having(" +
           (predicate_ != nullptr ? predicate_->GetInfo() : "") + "))";
  }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Utils
  ///
  //////////////////////////////////////////////////////////////////////////////

  void PerformBinding(BindingContext &binding_context) override;

  std::unique_ptr<AbstractPlan> Copy() const override;

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;

  void VisitParameters(
      codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

 private:
  /* For projection */
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  /* For HAVING clause */
  std::unique_ptr<const expression::AbstractExpression> predicate_;

  /* Unique aggregate terms */
  const std::vector<AggTerm> unique_agg_terms_;

  /* Group-by Keys */
  const std::vector<oid_t> groupby_col_ids_;
  std::vector<const AttributeInfo *> groupby_ais_;

  /* Output schema */
  std::shared_ptr<const catalog::Schema> output_schema_;

  /* Aggregate Strategy */
  const AggregateType agg_strategy_;

  /* Columns involved */
  std::vector<oid_t> column_ids_;

 private:
  DISALLOW_COPY_AND_MOVE(AggregatePlan);
};

}  // namespace planner
}  // namespace peloton
