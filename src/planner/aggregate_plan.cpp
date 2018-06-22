//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_plan.cpp
//
// Identification: src/planner/aggregate_plan.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/aggregate_plan.h"

#include "codegen/type/bigint_type.h"
#include "codegen/type/decimal_type.h"
#include "common/logger.h"

namespace peloton {
namespace planner {

////////////////////////////////////////////////////////////////////////////////
///
/// Aggregate Term
///
////////////////////////////////////////////////////////////////////////////////

AggregatePlan::AggTerm::AggTerm(
    ExpressionType et, std::unique_ptr<expression::AbstractExpression> &&expr,
    bool distinct)
    : agg_type(et), expression(std::move(expr)), distinct(distinct) {
  agg_ai.name = StringUtil::Format(
      "%s(%s)", ExpressionTypeToString(agg_type).c_str(),
      (expression != nullptr ? expression->GetInfo().c_str() : "*"));
}

void AggregatePlan::AggTerm::PerformBinding(bool is_global,
                                            BindingContext &binding_context) {
  // If there's an input expression, first perform binding
  if (expression != nullptr) {
    expression->PerformBinding({&binding_context});
  }

  // Setup the aggregate's return type
  switch (agg_type) {
    case ExpressionType::AGGREGATE_COUNT:
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      /*
       * The SQL type of COUNT() or COUNT(*) is always a non-nullable BIGINT
       */
      agg_ai.type =
          codegen::type::Type(codegen::type::BigInt::Instance(), false);
      break;
    }
    case ExpressionType::AGGREGATE_AVG: {
      /*
       * AVG() must have an input expression (that has been bound earlier). The
       * return type of the AVG() aggregate is always a SQL DECIMAL that may or
       * may not be NULL depending on the input expression.
       */
      PELOTON_ASSERT(expression != nullptr);
      // TODO: Move this logic into the SQL type
      const auto &input_type = expression->ResultType();
      bool nullable = input_type.nullable || is_global;

      agg_ai.type =
          codegen::type::Type(codegen::type::Decimal::Instance(), nullable);
      break;
    }
    case ExpressionType::AGGREGATE_MAX:
    case ExpressionType::AGGREGATE_MIN:
    case ExpressionType::AGGREGATE_SUM: {
      /*
       * These aggregates must have an input expression and takes on the same
       * return type as its input expression.
       */
      PELOTON_ASSERT(expression != nullptr);
      agg_ai.type = expression->ResultType();
      if (is_global) {
        agg_ai.type = agg_ai.type.AsNullable();
      }
      break;
    }
    default: {
      throw Exception(
          StringUtil::Format("%s not a valid aggregate",
                             ExpressionTypeToString(agg_type).c_str()));
    }
  }
}

hash_t AggregatePlan::AggTerm::Hash() const {
  hash_t hash = HashUtil::Hash(&agg_type);

  if (expression != nullptr) {
    hash = HashUtil::CombineHashes(hash, expression->Hash());
  }

  return HashUtil::CombineHashes(hash, HashUtil::Hash(&distinct));
}

bool AggregatePlan::AggTerm::operator==(const AggTerm &rhs) const {
  if (agg_type != rhs.agg_type) {
    return false;
  }

  if (expression != nullptr && *expression != *rhs.expression) {
    return false;
  }

  if (distinct != rhs.distinct) {
    return false;
  }

  return true;
}

AggregatePlan::AggTerm AggregatePlan::AggTerm::Copy() const {
  std::unique_ptr<expression::AbstractExpression> expr_copy(expression->Copy());
  return AggTerm(agg_type, std::move(expr_copy), distinct);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Aggregate Plan
///
////////////////////////////////////////////////////////////////////////////////

void AggregatePlan::PerformBinding(BindingContext &binding_context) {
  BindingContext input_context;

  const auto &children = GetChildren();
  PELOTON_ASSERT(children.size() == 1);

  children[0]->PerformBinding(input_context);

  PELOTON_ASSERT(groupby_ais_.empty());

  // First get bindings for the grouping keys
  for (oid_t gb_col_id : GetGroupbyColIds()) {
    auto *ai = input_context.Find(gb_col_id);
    LOG_DEBUG("Grouping col %u binds to AI %p", gb_col_id, ai);
    PELOTON_ASSERT(ai != nullptr);
    groupby_ais_.push_back(ai);
  }

  const auto &aggregates = GetUniqueAggTerms();

  // Now let the aggregate expressions do their bindings
  for (const auto &agg_term : GetUniqueAggTerms()) {
    auto &non_const_agg_term = const_cast<AggregatePlan::AggTerm &>(agg_term);
    non_const_agg_term.PerformBinding(IsGlobal(), input_context);
  }

  // Handle the projection by creating two binding contexts, the first being
  // input context we receive, and the next being all the aggregates this
  // plan produces.
  BindingContext agg_ctx;
  for (oid_t i = 0; i < aggregates.size(); i++) {
    const auto &agg_ai = aggregates[i].agg_ai;
    LOG_DEBUG("Binding aggregate at position %u to AI %p", i, &agg_ai);
    agg_ctx.BindNew(i, &agg_ai);
  }

  // Do projection (if one exists)
  if (GetProjectInfo() != nullptr) {
    std::vector<const BindingContext *> inputs = {&input_context, &agg_ctx};
    GetProjectInfo()->PerformRebinding(binding_context, inputs);
  }

  // Let the predicate do its binding (if one exists)
  const auto *predicate = GetPredicate();
  if (predicate != nullptr) {
    const_cast<expression::AbstractExpression *>(predicate)
        ->PerformBinding({&binding_context});
  }
}

std::unique_ptr<AbstractPlan> AggregatePlan::Copy() const {
  // Copy agg terms
  std::vector<AggTerm> copied_agg_terms;
  for (const auto &term : unique_agg_terms_) {
    copied_agg_terms.push_back(term.Copy());
  }

  // Copy grouping column IDs
  std::vector<oid_t> copied_groupby_col_ids(groupby_col_ids_);

  // Copy schema
  std::shared_ptr<const catalog::Schema> output_schema_copy(
      catalog::Schema::CopySchema(GetOutputSchema()));

  // Construct new plan
  AggregatePlan *new_plan = new AggregatePlan(
      project_info_->Copy(),
      std::unique_ptr<const expression::AbstractExpression>(predicate_->Copy()),
      std::move(copied_agg_terms), std::move(copied_groupby_col_ids),
      output_schema_copy, agg_strategy_);

  // Done
  return std::unique_ptr<AbstractPlan>(new_plan);
}

hash_t AggregatePlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  if (GetPredicate() != nullptr) {
    hash = HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  for (const auto &agg_term : GetUniqueAggTerms()) {
    hash = HashUtil::CombineHashes(hash, agg_term.Hash());
  }

  if (GetProjectInfo() != nullptr) {
    hash = HashUtil::CombineHashes(hash, GetProjectInfo()->Hash());
  }

  for (const oid_t gb_col_id : GetGroupbyColIds()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&gb_col_id));
  }

  hash = HashUtil::CombineHashes(hash, GetOutputSchema()->Hash());

  auto agg_strategy = GetAggregateStrategy();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&agg_strategy));

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool AggregatePlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }

  auto &other = static_cast<const planner::AggregatePlan &>(rhs);

  // Predicate
  if ((predicate_ == nullptr && other.predicate_ != nullptr) ||
      (predicate_ != nullptr && other.predicate_ == nullptr)) {
    return false;
  }

  if (predicate_ != nullptr && *predicate_ != *other.predicate_) {
    return false;
  }

  // UniqueAggTerms
  if (GetUniqueAggTerms() != other.GetUniqueAggTerms()) {
    return false;
  }

  // Project Info
  if ((project_info_ == nullptr && other.project_info_ != nullptr) ||
      (project_info_ != nullptr && other.project_info_ == nullptr)) {
    return false;
  }

  if (project_info_ != nullptr && *project_info_ != *other.project_info_) {
    return false;
  }

  // Group by
  if (GetGroupbyColIds() != other.GetGroupbyColIds()) {
    return false;
  }

  if (*GetOutputSchema() != *other.GetOutputSchema()) {
    return false;
  }

  if (GetAggregateStrategy() != other.GetAggregateStrategy()) {
    return false;
  }

  return (AbstractPlan::operator==(rhs));
}

void AggregatePlan::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  AbstractPlan::VisitParameters(map, values, values_from_user);

  for (const auto &agg_term : GetUniqueAggTerms()) {
    if (agg_term.expression != nullptr) {
      agg_term.expression->VisitParameters(map, values, values_from_user);
    }
  }

  if (!GetGroupbyColIds().empty()) {
    auto *predicate =
        const_cast<expression::AbstractExpression *>(GetPredicate());
    if (predicate != nullptr) {
      predicate->VisitParameters(map, values, values_from_user);
    }

    auto *proj_info = const_cast<planner::ProjectInfo *>(GetProjectInfo());
    if (proj_info != nullptr) {
      proj_info->VisitParameters(map, values, values_from_user);
    }
  }
}

}  // namespace planner
}  // namespace peloton
