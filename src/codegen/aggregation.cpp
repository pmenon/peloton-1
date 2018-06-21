//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregation.cpp
//
// Identification: src/codegen/aggregation.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/aggregation.h"

#include "codegen/lang/if.h"
#include "codegen/type/bigint_type.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/decimal_type.h"

namespace peloton {
namespace codegen {

void Aggregation::Setup(
    CodeGen &codegen,
    const std::vector<planner::AggregatePlan::AggTerm> &aggregates,
    bool is_global) {
  is_global_ = is_global;

  for (uint32_t source_idx = 0; source_idx < aggregates.size(); source_idx++) {
    const auto &agg_term = aggregates[source_idx];
    switch (agg_term.agg_type) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        /*
         * COUNT(...)'s can never be NULL.
         */
        const type::Type count_type(type::BigInt::Instance(), false);
        uint32_t storage_pos = storage_.AddType(count_type);

        aggregate_infos_.emplace_back(
            AggregateInfo{.aggregate_type = agg_term.agg_type,
                          .source_index = source_idx,
                          .storage_index = storage_pos,
                          .is_internal = false});
        break;
      }
      case ExpressionType::AGGREGATE_SUM: {
        /*
         * If we're doing a global aggregation, the aggregate can potentially be
         * NULL, for example, there are no input rows. Modify the type
         * appropriately.
         */
        auto value_type = agg_term.expression->ResultType();
        if (IsGlobal()) {
          value_type = value_type.AsNullable();
        }

        uint32_t storage_pos = storage_.AddType(value_type);

        aggregate_infos_.emplace_back(
            AggregateInfo{.aggregate_type = agg_term.agg_type,
                          .source_index = source_idx,
                          .storage_index = storage_pos,
                          .is_internal = false});
        break;
      }
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        /*
         * If we're doing a global aggregation, the aggregate can potentially be
         * NULL, for example, there are no input rows. Modify the type
         * appropriately.
         */
        auto value_type = agg_term.expression->ResultType();
        if (IsGlobal()) {
          value_type = value_type.AsNullable();
        }

        uint32_t storage_pos = storage_.AddType(value_type);

        aggregate_infos_.emplace_back(
            AggregateInfo{.aggregate_type = agg_term.agg_type,
                          .source_index = source_idx,
                          .storage_index = storage_pos,
                          .is_internal = false});
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        /*
         * We decompose averages into separate SUM() and COUNT() components. The
         * type for the SUM() aggregate must match the type of the input
         * expression we're summing over. COUNT() can never be NULL.
         *
         * If we're doing a global aggregation, the SUM() aggregate can
         * potentially be NULL if, for example, there are no input rows. Modify
         * the type appropriately.
         */
        type::Type count_type(type::BigInt::Instance(), false);
        type::Type sum_type = agg_term.expression->ResultType();
        if (IsGlobal()) {
          sum_type = sum_type.AsNullable();
        }

        uint32_t sum_storage_pos = storage_.AddType(sum_type);
        uint32_t count_storage_pos = storage_.AddType(count_type);

        /* Add the SUM(), COUNT() and (logical) AVG() aggregates */
        aggregate_infos_.emplace_back(
            AggregateInfo{.aggregate_type = ExpressionType::AGGREGATE_SUM,
                          .source_index = source_idx,
                          .storage_index = sum_storage_pos,
                          .is_internal = true});
        aggregate_infos_.emplace_back(
            AggregateInfo{.aggregate_type = ExpressionType::AGGREGATE_COUNT,
                          .source_index = source_idx,
                          .storage_index = count_storage_pos,
                          .is_internal = true});
        aggregate_infos_.emplace_back(
            AggregateInfo{.aggregate_type = ExpressionType::AGGREGATE_AVG,
                          .source_index = source_idx,
                          .storage_index = std::numeric_limits<uint32_t>::max(),
                          .is_internal = false});
        break;
      }
      default: {
        throw Exception(StringUtil::Format(
            "Unexpected aggregate type '%s' when preparing aggregation",
            ExpressionTypeToString(agg_term.agg_type).c_str()));
      }
    }
  }

  // Finalize the storage format
  storage_.Finalize(codegen);
}

void Aggregation::CreateInitialGlobalValues(CodeGen &codegen,
                                            llvm::Value *space) const {
  PELOTON_ASSERT(IsGlobal());
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);
  null_bitmap.InitAllNull(codegen);
  null_bitmap.WriteBack(codegen);
}

void Aggregation::CreateInitialValues(
    CodeGen &codegen, llvm::Value *space,
    const std::vector<codegen::Value> &initial_vals) const {
  // Global aggregations should be calling CreateInitialGlobalValues(...)
  PELOTON_ASSERT(!IsGlobal());

  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);

  // Initialize bitmap to all NULLs
  null_bitmap.InitAllNull(codegen);

  // Iterate over the aggregations
  for (const auto &agg_info : aggregate_infos_) {
    const auto &initial = initial_vals[agg_info.source_index];

    switch (agg_info.aggregate_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        // For the above aggregations, the initial value is the attribute value
        storage_.SetValue(codegen, space, agg_info.storage_index, initial,
                          null_bitmap);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT: {
        llvm::Value *raw_initial = nullptr;
        if (initial.IsNullable()) {
          llvm::Value *not_null = initial.IsNotNull(codegen);
          raw_initial = codegen->CreateZExt(not_null, codegen.Int64Type());
        } else {
          raw_initial = codegen.Const64(1);
        }

        const type::Type count_type(type::BigInt::Instance(), false);
        codegen::Value initial_val(count_type, raw_initial);
        storage_.SetValueSkipNull(codegen, space, agg_info.storage_index,
                                  initial_val);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Counts can never be NULL, so skip handling it
        storage_.SetValueSkipNull(codegen, space, agg_info.storage_index,
                                  initial);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // Nothing to do here
        break;
      }
      default: {
        throw Exception(StringUtil::Format(
            "Unexpected aggregate type '%s' when creating initial values",
            ExpressionTypeToString(agg_info.aggregate_type).c_str()));
      }
    }
  }

  // Write the final contents of the null bitmap
  null_bitmap.WriteBack(codegen);
}

void Aggregation::DoAdvanceValue(CodeGen &codegen, llvm::Value *space,
                                 const AggregateInfo &agg_info,
                                 const codegen::Value &update) const {
  PELOTON_ASSERT(agg_info.storage_index < std::numeric_limits<uint32_t>::max());

  // Load the current value of the aggregate
  auto curr = storage_.GetValueSkipNull(codegen, space, agg_info.storage_index);

  // Now compute the next value of the aggregate
  codegen::Value next;
  switch (agg_info.aggregate_type) {
    case ExpressionType::AGGREGATE_COUNT_STAR:
    case ExpressionType::AGGREGATE_SUM: {
      next = curr.Add(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_MIN: {
      next = curr.Min(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_MAX: {
      next = curr.Max(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_COUNT: {
      // Convert the next update into 0 or 1 depending on if it's NULL
      codegen::type::Type update_type(type::BigInt::Instance(), false);
      codegen::Value raw_update;
      if (update.IsNullable()) {
        llvm::Value *not_null = update.IsNotNull(codegen);
        llvm::Value *raw = codegen->CreateZExt(not_null, codegen.Int64Type());
        raw_update = codegen::Value(update_type, raw);
      } else {
        raw_update = codegen::Value(update_type, codegen.Const64(1));
      }

      // Add to aggregate
      next = curr.Add(codegen, raw_update);
      break;
    }
    default: {
      throw Exception(StringUtil::Format(
          "Unexpected aggregate type '%s' when advancing aggregator",
          ExpressionTypeToString(agg_info.aggregate_type).c_str()));
    }
  }

  // Store the updated value in the appropriate slot
  storage_.SetValueSkipNull(codegen, space, agg_info.storage_index, next);
}

void Aggregation::DoNullCheck(
    CodeGen &codegen, llvm::Value *space, const AggregateInfo &agg_info,
    const codegen::Value &update,
    UpdateableStorage::NullBitmap &null_bitmap) const {
  /*
   * This aggregate is NULL-able, we need to check if the update value is
   * NULL, and whether the current value of the aggregate is NULL.
   *
   * There are two cases we handle:
   * (1). If neither the update value or the current aggregate value are NULL,
   *      we simple do the regular aggregation without NULL checking.
   * (2). If the update value is not NULL, but the current aggregate **is**
   *      NULL, then we just store this update value as if we're creating it
   *      for the first time.
   *
   * If either the update value or the current aggregate are NULL, we have
   * nothing to do.
   */

  PELOTON_ASSERT(agg_info.storage_index < std::numeric_limits<uint32_t>::max());

  // Fetch null byte so we can phi-resolve it after all the branches
  llvm::Value *null_byte_snapshot =
      null_bitmap.ByteFor(codegen, agg_info.storage_index);

  lang::If valid_update(codegen, update.IsNotNull(codegen));
  {
    lang::If agg_is_null(codegen,
                         null_bitmap.IsNull(codegen, agg_info.storage_index));
    {
      /* Case (2): update is not NULL, but aggregate is NULL */
      storage_.SetValue(codegen, space, agg_info.storage_index, update,
                        null_bitmap);
    }
    agg_is_null.ElseBlock();
    {
      /* Case (1): both update and aggregate are not NULL */
      DoAdvanceValue(codegen, space, agg_info, update);
    }
    agg_is_null.EndIf();

    // Merge the null value
    null_bitmap.MergeValues(agg_is_null, null_byte_snapshot);
  }
  valid_update.EndIf();

  // Merge the null value
  null_bitmap.MergeValues(valid_update, null_byte_snapshot);
}

void Aggregation::AdvanceValues(
    CodeGen &codegen, llvm::Value *space,
    const std::vector<codegen::Value> &next_vals) const {
  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);

  for (const auto &agg_info : aggregate_infos_) {
    /*
     * We don't care about averages directly at this point. Their individual
     * components will get updated appropriately.
     */
    if (agg_info.aggregate_type == ExpressionType::AGGREGATE_AVG) {
      continue;
    }

    // The update value
    const codegen::Value &update = next_vals[agg_info.source_index];

    // Check nullability
    if (!null_bitmap.IsNullable(agg_info.storage_index)) {
      DoAdvanceValue(codegen, space, agg_info, update);
    } else {
      DoNullCheck(codegen, space, agg_info, update, null_bitmap);
    }
  }

  // Write the final contents of the null bitmap
  null_bitmap.WriteBack(codegen);
}

void Aggregation::FinalizeValues(
    CodeGen &codegen, llvm::Value *space,
    std::vector<codegen::Value> &final_vals) const {
  std::map<std::pair<uint32_t, ExpressionType>, codegen::Value> internal_vals;

  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);

  for (const auto &agg_info : aggregate_infos_) {
    ExpressionType agg_type = agg_info.aggregate_type;
    switch (agg_type) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Neither COUNT(...) or COUNT(*) can ever return NULL, so no NULL-check
        codegen::Value final_val =
            storage_.GetValueSkipNull(codegen, space, agg_info.storage_index);

        if (agg_info.is_internal) {
          internal_vals.emplace(std::make_pair(agg_info.source_index, agg_type),
                                final_val);
        } else {
          final_vals.push_back(final_val);
        }

        break;
      }
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        codegen::Value final_val = storage_.GetValue(
            codegen, space, agg_info.storage_index, null_bitmap);

        if (agg_info.is_internal) {
          internal_vals.emplace(std::make_pair(agg_info.source_index, agg_type),
                                final_val);
        } else {
          final_vals.push_back(final_val);
        }

        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // Collect the final values of the SUM and the COUNT components
        codegen::Value sum = internal_vals[std::make_pair(
            agg_info.source_index, ExpressionType::AGGREGATE_SUM)];

        codegen::Value count = internal_vals[std::make_pair(
            agg_info.source_index, ExpressionType::AGGREGATE_COUNT)];

        // Cast the values to DECIMAL
        codegen::Value sum_casted =
            sum.CastTo(codegen, type::Decimal::Instance());
        codegen::Value count_casted =
            count.CastTo(codegen, type::Decimal::Instance());

        // Compute the average
        codegen::Value final_val =
            sum_casted.Div(codegen, count_casted, OnError::ReturnNull);

        if (agg_info.is_internal) {
          internal_vals.emplace(std::make_pair(agg_info.source_index, agg_type),
                                final_val);
        } else {
          final_vals.push_back(final_val);
        }

        break;
      }
      default: {
        throw Exception(StringUtil::Format(
            "Unexpected aggregate type '%s' when finalizing aggregator",
            ExpressionTypeToString(agg_type).c_str()));
      }
    }
  }
}

void Aggregation::DoMergePartial(CodeGen &codegen,
                                 const Aggregation::AggregateInfo &agg_info,
                                 llvm::Value *curr_vals,
                                 llvm::Value *new_vals) const {
  PELOTON_ASSERT(agg_info.storage_index < std::numeric_limits<uint32_t>::max());

  codegen::Value curr =
      storage_.GetValueSkipNull(codegen, curr_vals, agg_info.storage_index);

  codegen::Value partial_val =
      storage_.GetValueSkipNull(codegen, new_vals, agg_info.storage_index);

  codegen::Value next;
  switch (agg_info.aggregate_type) {
    case ExpressionType::AGGREGATE_COUNT:
    case ExpressionType::AGGREGATE_COUNT_STAR:
    case ExpressionType::AGGREGATE_SUM: {
      next = curr.Add(codegen, partial_val);
      break;
    }
    case ExpressionType::AGGREGATE_MIN: {
      next = curr.Min(codegen, partial_val);
      break;
    }
    case ExpressionType::AGGREGATE_MAX: {
      next = curr.Max(codegen, partial_val);
      break;
    }
    default: {
      throw Exception(StringUtil::Format(
          "Unexpected aggregate type '%s' when merging partial aggregator",
          ExpressionTypeToString(agg_info.aggregate_type).c_str()));
    }
  }

  storage_.SetValueSkipNull(codegen, curr_vals, agg_info.storage_index, next);
}

void Aggregation::MergePartialAggregates(CodeGen &codegen,
                                         llvm::Value *curr_vals,
                                         llvm::Value *new_vals) const {
  UpdateableStorage::NullBitmap curr_null_bitmap(codegen, storage_, curr_vals);
  UpdateableStorage::NullBitmap new_null_bitmap(codegen, storage_, new_vals);
  for (const auto &agg_info : aggregate_infos_) {
    /*
     * We don't care about averages directly at this point. Their individual
     * components will get merged appropriately.
     */
    if (agg_info.aggregate_type == ExpressionType::AGGREGATE_AVG) {
      continue;
    }

    /*
     * If neither the current nor the new partial aggregate slot is NULLable,
     * use the fast path to merge these values together.
     */
    if (!curr_null_bitmap.IsNullable(agg_info.storage_index)) {
      DoMergePartial(codegen, agg_info, curr_vals, new_vals);
      continue;
    }

    /*
     * Here, both the current aggregate value and the partial aggregate can be
     * NULL. If the partial aggregate is NULL, we don't need to do anything. If
     * the partial aggregate is not NULL, but the current aggregate value is, we
     * overwrite the current value with the partial. If both are non-NULL, we
     * do a proper merge.
     */

    uint32_t storage_idx = agg_info.storage_index;

    llvm::Value *null_byte_snapshot =
        curr_null_bitmap.ByteFor(codegen, storage_idx);

    llvm::Value *partial_not_null =
        codegen->CreateNot(new_null_bitmap.IsNull(codegen, storage_idx));
    lang::If valid_partial(codegen, partial_not_null);
    {
      llvm::Value *current_null = curr_null_bitmap.IsNull(codegen, storage_idx);
      lang::If curr_is_null(codegen, current_null);
      {
        auto partial =
            storage_.GetValueSkipNull(codegen, new_vals, storage_idx);
        storage_.SetValue(codegen, curr_vals, storage_idx, partial,
                          curr_null_bitmap);
      }
      curr_is_null.ElseBlock();
      {
        // Normal merge
        DoMergePartial(codegen, agg_info, curr_vals, new_vals);
      }
      curr_is_null.EndIf();

      curr_null_bitmap.MergeValues(curr_is_null, null_byte_snapshot);
    }
    valid_partial.EndIf();

    curr_null_bitmap.MergeValues(valid_partial, null_byte_snapshot);
  }
}

void Aggregation::MergeDistinct(CodeGen &codegen, llvm::Value *space,
                                uint32_t index,
                                const codegen::Value &val) const {
  // TODO: Implement me
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);
  (void)codegen;
  (void)space;
  (void)index;
  (void)val;
}

}  // namespace codegen
}  // namespace peloton
