//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// projection_plan.cpp
//
// Identification: src/planner/projection_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/projection_plan.h"

#include "common/logger.h"

namespace peloton {
namespace planner {

ProjectionPlan::ProjectionPlan(
    std::unique_ptr<const planner::ProjectInfo> &&project_info,
    const std::shared_ptr<const catalog::Schema> &schema)
    : project_info_(std::move(project_info)), schema_(schema) {}

void ProjectionPlan::PerformBinding(BindingContext &context) {
  const auto& children = GetChildren();
  if (children.empty())
    return;
  PELOTON_ASSERT(children.size() == 1);

  // Let the child do its binding first
  BindingContext child_context;
  children[0]->PerformBinding(child_context);

  std::vector<const BindingContext *> inputs = {&child_context};
  GetProjectInfo()->PerformRebinding(context, inputs);
}

hash_t ProjectionPlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  hash = HashUtil::CombineHashes(hash, GetProjectInfo()->Hash());

  hash = HashUtil::CombineHashes(hash, GetSchema()->Hash());

  for (const oid_t col_id : GetColumnIds()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&col_id));
  }

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool ProjectionPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }

  auto &other = static_cast<const planner::ProjectionPlan &>(rhs);

  // Compare projection
  if ((project_info_ == nullptr && other.project_info_ != nullptr) ||
      (project_info_ != nullptr && other.project_info_ == nullptr)) {
    return false;
  }

  if (project_info_ != nullptr && *project_info_ != *other.project_info_) {
    return false;
  }

  // Compare schemas
  if (*schema_ != *other.schema_) {
    return false;
  }

  // Compare column_ids
  if (column_ids_ != other.column_ids_) {
    return false;
  }

  return AbstractPlan::operator==(rhs);
}

void ProjectionPlan::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  AbstractPlan::VisitParameters(map, values, values_from_user);

  auto *proj_info = const_cast<planner::ProjectInfo *>(GetProjectInfo());
  proj_info->VisitParameters(map, values, values_from_user);
}

}  // namespace planner
}  // namespace peloton
