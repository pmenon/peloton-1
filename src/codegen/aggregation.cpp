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
#include "codegen/type/integer_type.h"
#include "codegen/type/decimal_type.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// An anonymous namespace with some handy functions for advancing aggregates
/// and merging in partial aggregates.
///
////////////////////////////////////////////////////////////////////////////////

namespace {

/**
 *
 * @param codegen
 * @param agg_info
 * @return
 */
codegen::Value InitialDistinctValue(
    CodeGen &codegen, const Aggregation::AggregateInfo &agg_info) {
#if 0
  switch (agg_info.aggregate_type) {
    case ExpressionType::AGGREGATE_COUNT: return codegen
  }
#endif
  // TODO: Implement me
  (void)agg_info;
  type::Type type(type::Integer::Instance(), /* nullable */ false);
  return codegen::Value(type, codegen.Const16(0));
}

/**
 * Advance the aggregate value assuming the current aggregate value IS NOT
 * NULL. The delta update value may or may not be NULL.
 *
 * @param codegen The codegen instance
 * @param storage
 * @param space A pointer to where all aggregates are contiguously stored
 * @param agg_info The aggregate (and information) to update
 * @param next The delta value we advance the aggregate by
 */
void AdvanceValue(CodeGen &codegen, const UpdateableStorage &storage,
                  llvm::Value *space,
                  const Aggregation::AggregateInfo &agg_info,
                  const codegen::Value &update) {
  PELOTON_ASSERT(agg_info.storage_index < std::numeric_limits<uint32_t>::max());

  const uint32_t storage_idx = agg_info.storage_index;

  auto curr = storage.GetValueSkipNull(codegen, space, storage_idx);

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

  storage.SetValueSkipNull(codegen, space, storage_idx, next);
}

/**
 *
 * @param codegen
 * @param storage
 * @param agg_info
 * @param curr_vals
 * @param new_vals
 */
void MergePartial(CodeGen &codegen, const UpdateableStorage &storage,
                  const Aggregation::AggregateInfo &agg_info,
                  llvm::Value *curr_vals, llvm::Value *new_vals) {
  PELOTON_ASSERT(agg_info.storage_index < std::numeric_limits<uint32_t>::max());

  const uint32_t storage_idx = agg_info.storage_index;

  const auto curr = storage.GetValueSkipNull(codegen, curr_vals, storage_idx);

  const auto partial = storage.GetValueSkipNull(codegen, new_vals, storage_idx);

  codegen::Value next;
  switch (agg_info.aggregate_type) {
    case ExpressionType::AGGREGATE_COUNT:
    case ExpressionType::AGGREGATE_COUNT_STAR:
    case ExpressionType::AGGREGATE_SUM: {
      next = curr.Add(codegen, partial);
      break;
    }
    case ExpressionType::AGGREGATE_MIN: {
      next = curr.Min(codegen, partial);
      break;
    }
    case ExpressionType::AGGREGATE_MAX: {
      next = curr.Max(codegen, partial);
      break;
    }
    default: {
      throw Exception(StringUtil::Format(
          "Unexpected aggregate type '%s' when merging partial aggregator",
          ExpressionTypeToString(agg_info.aggregate_type).c_str()));
    }
  }

  storage.SetValueSkipNull(codegen, curr_vals, storage_idx, next);
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////
///
/// Aggregate information
///
////////////////////////////////////////////////////////////////////////////////

Aggregation::AggregateInfo::AggregateInfo(const ExpressionType _aggregate_type,
                                          const uint32_t _source_index,
                                          const uint32_t _storage_index,
                                          bool _internal, bool _distinct)
    : aggregate_type(_aggregate_type),
      source_index(_source_index),
      storage_index(_storage_index),
      internal(_internal),
      distinct(_distinct) {}

////////////////////////////////////////////////////////////////////////////////
///
/// Aggregation
///
////////////////////////////////////////////////////////////////////////////////

Aggregation::Aggregation() : storage_("AggBuf") {}

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
         * COUNT(...)'s can never be NULL and their type is always BIGINT.
         */
        const type::Type count_type(type::BigInt::Instance(),
                                    /* nullable */ false);
        uint32_t storage_pos = storage_.AddType(count_type);

        bool distinct = agg_term.distinct &&
                        agg_term.agg_type == ExpressionType::AGGREGATE_COUNT;

        aggregate_infos_.emplace_back(agg_term.agg_type, source_idx,
                                      storage_pos, /* internal */ false,
                                      distinct);
        break;
      }
      case ExpressionType::AGGREGATE_SUM: {
        /*
         * If we're doing a global aggregation, the aggregate can potentially be
         * NULL even though the underlying aggregate expression is non-NULLable.
         * This can happen, for example, if there are no input rows. Modify the
         * type appropriately.
         */
        auto value_type = agg_term.expression->ResultType();
        if (is_global) {
          value_type = value_type.AsNullable();
        }

        uint32_t storage_pos = storage_.AddType(value_type);

        aggregate_infos_.emplace_back(agg_term.agg_type, source_idx,
                                      storage_pos, /* internal */ false,
                                      agg_term.distinct);
        break;
      }
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        /*
         * If we're doing a global aggregation, the aggregate can potentially be
         * NULL even though the underlying aggregate expression is non-NULLable.
         * This can happen, for example, if there are no input rows. Modify the
         * type appropriately.
         *
         * While MIN/MAX aggregates can be distinct, they require no special
         * handling over their non-distinct versions. Hence, we don't set the
         * distinct flag for them here.
         */
        auto value_type = agg_term.expression->ResultType();
        if (is_global) {
          value_type = value_type.AsNullable();
        }

        uint32_t storage_pos = storage_.AddType(value_type);

        aggregate_infos_.emplace_back(agg_term.agg_type, source_idx,
                                      storage_pos, /* internal */ false,
                                      /* distinct */ false);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        /*
         * We decompose averages into separate SUM() and COUNT() components. The
         * type for the SUM() aggregate must match the type of the input
         * expression we're summing over. COUNT()'s are non-NULLable BIGINTs.
         *
         * If we're doing a global aggregation, the aggregate can potentially be
         * NULL even though the underlying aggregate expression is non-NULLable.
         * This can happen, for example, if there are no input rows. Modify the
         * type appropriately.
         */
        type::Type count_type(type::BigInt::Instance(), false);
        type::Type sum_type = agg_term.expression->ResultType();
        if (is_global) {
          sum_type = sum_type.AsNullable();
        }

        uint32_t sum_storage_pos = storage_.AddType(sum_type);
        uint32_t count_storage_pos = storage_.AddType(count_type);

        /* Add the SUM(), COUNT() and (logical) AVG() aggregates */
        aggregate_infos_.emplace_back(ExpressionType::AGGREGATE_SUM, source_idx,
                                      sum_storage_pos, /* internal */ true,
                                      agg_term.distinct);
        aggregate_infos_.emplace_back(ExpressionType::AGGREGATE_COUNT,
                                      source_idx, count_storage_pos,
                                      /* internal */ true, agg_term.distinct);
        aggregate_infos_.emplace_back(ExpressionType::AGGREGATE_AVG, source_idx,
                                      std::numeric_limits<uint32_t>::max(),
                                      /* internal */ false, agg_term.distinct);
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
  PELOTON_ASSERT(is_global_);
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);
  null_bitmap.InitAllNull(codegen);
  null_bitmap.WriteBack(codegen);
}

void Aggregation::CreateInitialValues(
    CodeGen &codegen, llvm::Value *space,
    const std::vector<codegen::Value> &initial_vals) const {
  PELOTON_ASSERT(!is_global_);

  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);

  null_bitmap.InitAllNull(codegen);

  for (const auto &agg_info : aggregate_infos_) {
    auto initial = agg_info.distinct ? InitialDistinctValue(codegen, agg_info)
                                     : initial_vals[agg_info.source_index];

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

  null_bitmap.WriteBack(codegen);
}

void Aggregation::AdvanceSingleValue(
    CodeGen &codegen, llvm::Value *space,
    const Aggregation::AggregateInfo &agg_info, const codegen::Value &next,
    UpdateableStorage::NullBitmap &null_bitmap) const {
  /* If the aggregate isn't NULL-able, use the fast path to advance. */
  if (!null_bitmap.IsNullable(agg_info.storage_index)) {
    AdvanceValue(codegen, storage_, space, agg_info, next);
    return;
  }

  /*
   * This aggregate is NULL-able, we need to perform some NULL checking. We need
   * to handle two scenarios which both consider only when the update/next value
   * is non-NULL:
   *  1. If the current aggregate value is NULL, the update value becomes the
   *     new value of the running aggregate.
   *  2. If the current aggregate is not NULL, we need to advance it by the next
   *     value.
   *
   * If the update value is NULL, we can skip it.
   */

  PELOTON_ASSERT(agg_info.storage_index < std::numeric_limits<uint32_t>::max());

  llvm::Value *null_byte_snapshot =
      null_bitmap.ByteFor(codegen, agg_info.storage_index);

  lang::If update_not_null(codegen, next.IsNotNull(codegen));
  {
    lang::If agg_is_null(codegen,
                         null_bitmap.IsNull(codegen, agg_info.storage_index));
    {
      storage_.SetValue(codegen, space, agg_info.storage_index, next,
                        null_bitmap);
    }
    agg_is_null.ElseBlock();
    {
      // Perform proper merge
      AdvanceValue(codegen, storage_, space, agg_info, next);
    }
    agg_is_null.EndIf();
    null_bitmap.MergeValues(agg_is_null, null_byte_snapshot);
  }
  update_not_null.EndIf();
  null_bitmap.MergeValues(update_not_null, null_byte_snapshot);
}

void Aggregation::AdvanceValues(
    CodeGen &codegen, llvm::Value *space,
    const std::vector<codegen::Value> &next_vals) const {
  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);

  for (const auto &agg_info : aggregate_infos_) {
    /* Skip derivative aggregates. Their values will be finalized later */
    if (agg_info.aggregate_type == ExpressionType::AGGREGATE_AVG) {
      continue;
    }

    /* Skip distinct aggregates. Their values will be merged in later */
    if (agg_info.distinct) {
      continue;
    }

    // Advance
    AdvanceSingleValue(codegen, space, agg_info,
                       next_vals[agg_info.source_index], null_bitmap);
  }

  // Write the final contents of the null bitmap
  null_bitmap.WriteBack(codegen);
}

void Aggregation::AdvanceDistinctValue(CodeGen &codegen, llvm::Value *space,
                                       uint32_t index,
                                       const codegen::Value &val) const {
  PELOTON_ASSERT(index < aggregate_infos_.size());

  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);

  for (const auto &agg_info : aggregate_infos_) {
    /* Skip derivative aggregates. Their values will be finalized later */
    if (agg_info.aggregate_type == ExpressionType::AGGREGATE_AVG) {
      continue;
    }

    /* Skip unrelated aggregates */
    if (agg_info.source_index != index) {
      continue;
    }

    /* Advance this single aggregate */
    AdvanceSingleValue(codegen, space, agg_info, val, null_bitmap);
  }

  null_bitmap.WriteBack(codegen);
}

void Aggregation::MergePartialAggregates(CodeGen &codegen,
                                         llvm::Value *curr_vals,
                                         llvm::Value *new_vals) const {
  UpdateableStorage::NullBitmap curr_null_bitmap(codegen, storage_, curr_vals);
  UpdateableStorage::NullBitmap new_null_bitmap(codegen, storage_, new_vals);
  for (const auto &agg_info : aggregate_infos_) {
    /* Skip derivative aggregates. Their values will be finalized later */
    if (agg_info.aggregate_type == ExpressionType::AGGREGATE_AVG) {
      continue;
    }

    /* If the aggregate isn't NULL-able, use the fast path to merge. */
    if (!curr_null_bitmap.IsNullable(agg_info.storage_index)) {
      MergePartial(codegen, storage_, agg_info, curr_vals, new_vals);
      continue;
    }

    /*
     * The aggregate at this position is NULL-able. If the partial aggregate is
     * NULL, we needn't do anything. If the partial aggregate is not NULL, but
     * the current aggregate is NULL, we overwrite the current value with the
     * partial. If both are non-NULL, we do a proper merge.
     */

    uint32_t storage_idx = agg_info.storage_index;

    llvm::Value *null_byte_snapshot =
        curr_null_bitmap.ByteFor(codegen, storage_idx);

    llvm::Value *partial_null = new_null_bitmap.IsNull(codegen, storage_idx);
    lang::If partial_not_null(codegen, codegen->CreateNot(partial_null));
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
        MergePartial(codegen, storage_, agg_info, curr_vals, new_vals);
      }
      curr_is_null.EndIf();
      curr_null_bitmap.MergeValues(curr_is_null, null_byte_snapshot);
    }
    partial_not_null.EndIf();
    curr_null_bitmap.MergeValues(partial_not_null, null_byte_snapshot);
  }
}

void Aggregation::FinalizeValues(
    CodeGen &codegen, llvm::Value *space,
    std::vector<codegen::Value> &final_vals) const {
  std::map<std::pair<uint32_t, ExpressionType>, codegen::Value> all_vals;

  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);

  for (const auto &agg_info : aggregate_infos_) {
    codegen::Value final_val;

    ExpressionType agg_type = agg_info.aggregate_type;
    switch (agg_type) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Neither COUNT(...) or COUNT(*) can ever return NULL, so no NULL-check
        final_val =
            storage_.GetValueSkipNull(codegen, space, agg_info.storage_index);
        break;
      }
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        final_val = storage_.GetValue(codegen, space, agg_info.storage_index,
                                      null_bitmap);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // Collect the final values of the SUM and the COUNT components
        codegen::Value sum = all_vals[std::make_pair(
            agg_info.source_index, ExpressionType::AGGREGATE_SUM)];

        codegen::Value count = all_vals[std::make_pair(
            agg_info.source_index, ExpressionType::AGGREGATE_COUNT)];

        // Cast the values to DECIMAL
        codegen::Value sum_casted =
            sum.CastTo(codegen, type::Decimal::Instance());
        codegen::Value count_casted =
            count.CastTo(codegen, type::Decimal::Instance());

        // Compute the average
        final_val = sum_casted.Div(codegen, count_casted, OnError::ReturnNull);
        break;
      }
      default: {
        throw Exception(StringUtil::Format(
            "Unexpected aggregate type '%s' when finalizing aggregator",
            ExpressionTypeToString(agg_type).c_str()));
      }
    }

    // Insert into global map
    all_vals.emplace(std::make_pair(agg_info.source_index, agg_type),
                     final_val);

    // If the aggregate isn't internal, push the value out
    if (!agg_info.internal) {
      final_vals.push_back(final_val);
    }
  }
}

}  // namespace codegen
}  // namespace peloton
