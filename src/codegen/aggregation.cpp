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
#include "codegen/type/integer_type.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// An anonymous namespace with some handy functions for advancing aggregates
/// and merging in partial aggregates.
///
////////////////////////////////////////////////////////////////////////////////

namespace {

codegen::Value InitialAggValue(CodeGen &codegen,
                               const Aggregation::AggregateInfo &agg_info,
                               const codegen::Value *initial) {
  switch (agg_info.agg_type) {
    case ExpressionType::AGGREGATE_COUNT: {
      if (initial == nullptr) {
        return codegen::Value(type::BigInt::Instance(), codegen.Const64(0));
      }

      const type::Type count_type(type::BigInt::Instance(), false);

      if (initial->IsNullable()) {
        llvm::Value *not_null = initial->IsNotNull(codegen);
        return codegen::Value(
            count_type, codegen->CreateZExt(not_null, codegen.Int64Type()));
      } else {
        return codegen::Value(count_type, codegen.Const64(1));
      }
    }
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      return codegen::Value(type::BigInt::Instance(), codegen.Const64(1));
    }
    case ExpressionType::AGGREGATE_MAX:
    case ExpressionType::AGGREGATE_MIN: {
      PELOTON_ASSERT(initial != nullptr);
      return *initial;
    }
    case ExpressionType::AGGREGATE_SUM: {
      if (initial != nullptr) {
        if (initial->GetType() == agg_info.type) {
          return *initial;
        } else {
          return initial->CastTo(codegen, agg_info.type);
        }
      }

      if (agg_info.type.type_id == peloton::type::TypeId::DECIMAL) {
        return codegen::Value(type::Decimal::Instance(),
                              codegen.ConstDouble(0.0));
      } else {
        return codegen::Value(type::BigInt::Instance(), codegen.Const64(0));
      }
    }
    default: {
      throw Exception(StringUtil::Format(
          "Unexpected aggregate type '%s' when finding base aggregate value",
          ExpressionTypeToString(agg_info.agg_type).c_str()));
    }
  }
}

/**
 * Advance the aggregate value assuming the current aggregate value IS NOT
 * NULL. The delta update value may or may not be NULL.
 *
 * @param codegen The codegen instance
 * @param storage The storage format of the aggregates
 * @param space A pointer to where all aggregates are contiguously stored
 * @param agg_info The aggregate (and information) to update
 * @param next The delta value we advance the aggregate by
 */
void AdvanceAgg(CodeGen &codegen, const UpdateableStorage &storage,
                llvm::Value *space, const Aggregation::AggregateInfo &agg_info,
                const codegen::Value &update) {
  PELOTON_ASSERT(agg_info.storage_idx < std::numeric_limits<uint32_t>::max());

  const uint32_t storage_idx = agg_info.storage_idx;

  codegen::Value curr = storage.GetValueSkipNull(codegen, space, storage_idx);

  codegen::Value next;
  switch (agg_info.agg_type) {
    case ExpressionType::AGGREGATE_SUM: {
      auto target_type = Aggregation::SumType(update.GetType());
      next = curr.Add(codegen, update.CastTo(codegen, target_type));
      break;
    }
    case ExpressionType::AGGREGATE_COUNT_STAR: {
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
          ExpressionTypeToString(agg_info.agg_type).c_str()));
    }
  }

  storage.SetValueSkipNull(codegen, space, storage_idx, next);
}

/**
 * Merge the contents of two partial aggregates, storing the result into the
 * destination aggregate space.
 *
 * @param codegen The codegen instance
 * @param storage The storage format of the aggregates
 * @param agg_info Information about the current aggregate we're merging
 * @param dest_aggs The aggregate space where we write the results of the merge
 * @param src_aggs Partial aggregates that we merge
 */
void MergePartialAgg(CodeGen &codegen, const UpdateableStorage &storage,
                     const Aggregation::AggregateInfo &agg_info,
                     llvm::Value *dest_aggs, llvm::Value *src_aggs) {
  PELOTON_ASSERT(agg_info.storage_idx < std::numeric_limits<uint32_t>::max());

  const uint32_t storage_idx = agg_info.storage_idx;

  const auto curr = storage.GetValueSkipNull(codegen, dest_aggs, storage_idx);

  const auto partial = storage.GetValueSkipNull(codegen, src_aggs, storage_idx);

  codegen::Value next;
  switch (agg_info.agg_type) {
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
          ExpressionTypeToString(agg_info.agg_type).c_str()));
    }
  }

  storage.SetValueSkipNull(codegen, dest_aggs, storage_idx, next);
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////
///
/// Aggregation
///
////////////////////////////////////////////////////////////////////////////////

Aggregation::Aggregation() : storage_("AggBuf") {}

type::Type Aggregation::SumType(const type::Type &input_type) {
  /*
   * We use BIGINT for TINYINT, SMALLINT, INTEGER and BIGINT. We use DECIMAL
   * for all others.
   *
   * TODO: We need to be more precise on what specifics of the DECIMAL we need,
   * such as precision etc.
   */
  switch (input_type.type_id) {
    case peloton::type::TypeId::TINYINT:
    case peloton::type::TypeId::SMALLINT:
    case peloton::type::TypeId::INTEGER:
    case peloton::type::TypeId::BIGINT: {
      return type::Type(type::BigInt::Instance(), input_type.nullable);
    }
    case peloton::type::TypeId::DECIMAL: {
      return input_type;
    }
    default: {
      throw Exception(StringUtil::Format("Type [%s] cannot be in SUM() agg",
                                         TypeIdToString(input_type.type_id)));
    }
  }
}

type::Type Aggregation::DividingAggType(const type::Type &numerator_type,
                                        const type::Type &denominator_type) {
  bool result_nullable = numerator_type.nullable || denominator_type.nullable;
  return type::Type(type::Decimal::Instance(), result_nullable);
}

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
        const type::Type count_type(type::BigInt::Instance(), false);
        uint32_t storage_pos = storage_.AddType(count_type);

        aggregate_infos_.emplace_back(
            agg_term.agg_type, count_type, count_type, source_idx, storage_pos,
            /* internal */ false, agg_term.distinct, /* derived */ false);
        break;
      }
      case ExpressionType::AGGREGATE_SUM: {
        /*
         * If we're doing a global aggregation, the aggregate can potentially be
         * NULL even though the underlying aggregate expression is non-NULLable.
         * This can happen, for example, if there are no input rows. Modify the
         * type appropriately.
         */
        type::Type input_type = agg_term.expression->ResultType();
        type::Type sum_type = SumType(agg_term.expression->ResultType());
        if (is_global) {
          sum_type = sum_type.AsNullable();
        }

        uint32_t storage_pos = storage_.AddType(sum_type);

        aggregate_infos_.emplace_back(
            agg_term.agg_type, sum_type, input_type, source_idx, storage_pos,
            /* internal */ false, agg_term.distinct, /* derived */ false);
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
        type::Type value_type = agg_term.expression->ResultType();
        if (is_global) {
          value_type = value_type.AsNullable();
        }

        uint32_t storage_pos = storage_.AddType(value_type);

        aggregate_infos_.emplace_back(agg_term.agg_type, value_type, value_type,
                                      source_idx, storage_pos,
                                      /* internal */ false,
                                      /* distinct */ false,
                                      /* derived */ false);
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
        type::Type input_type = agg_term.expression->ResultType();
        type::Type sum_type = SumType(input_type);
        if (is_global) {
          sum_type = sum_type.AsNullable();
        }
        type::Type avg_type = DividingAggType(sum_type, count_type);

        uint32_t sum_storage_pos = storage_.AddType(sum_type);
        uint32_t count_storage_pos = storage_.AddType(count_type);

        /* Add the SUM(), COUNT() and (logical) AVG() aggregates */
        aggregate_infos_.emplace_back(ExpressionType::AGGREGATE_SUM, sum_type,
                                      input_type, source_idx, sum_storage_pos,
                                      /* internal */ true, agg_term.distinct,
                                      /* derived */ false);
        aggregate_infos_.emplace_back(ExpressionType::AGGREGATE_COUNT,
                                      count_type, count_type, source_idx,
                                      count_storage_pos,
                                      /* internal */ true, agg_term.distinct,
                                      /* derived */ false);
        aggregate_infos_.emplace_back(ExpressionType::AGGREGATE_AVG, avg_type,
                                      avg_type, source_idx,
                                      std::numeric_limits<uint32_t>::max(),
                                      /* internal */ false, agg_term.distinct,
                                      /* derived */ true);
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
                                            llvm::Value *aggs) const {
  PELOTON_ASSERT(is_global_);
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, aggs);
  null_bitmap.InitAllNull(codegen);
  null_bitmap.WriteBack(codegen);
}

void Aggregation::CreateInitialValues(
    CodeGen &codegen, llvm::Value *aggs,
    const std::vector<codegen::Value> &initial_vals) const {
  PELOTON_ASSERT(!is_global_);

  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, aggs);

  null_bitmap.InitAllNull(codegen);

  for (const auto &agg_info : aggregate_infos_) {
    // Skip derivative aggregates. Their component values are initialized later.
    if (agg_info.derived) {
      continue;
    }

    // Compute the initial value of the aggregate
    auto initial = InitialAggValue(
        codegen, agg_info,
        agg_info.distinct ? nullptr : &initial_vals[agg_info.source_idx]);

    // Write it out
    storage_.SetValue(codegen, aggs, agg_info.storage_idx, initial,
                      null_bitmap);
  }

  null_bitmap.WriteBack(codegen);
}

void Aggregation::AdvanceValue(
    CodeGen &codegen, llvm::Value *space,
    const Aggregation::AggregateInfo &agg_info, const codegen::Value &next,
    UpdateableStorage::NullBitmap &null_bitmap) const {
  /*
   * If the aggregate isn't NULL-able, use the fast path to advance.
   */
  if (!null_bitmap.IsNullable(agg_info.storage_idx)) {
    AdvanceAgg(codegen, storage_, space, agg_info, next);
    return;
  }

  /*
   * This aggregate is NULL-able, we need to perform some NULL checking. We need
   * to handle two scenarios which both consider only when the update value
   * is non-NULL:
   *  1. If the current aggregate value is NULL, the update value becomes the
   *     new value of the running aggregate.
   *  2. If the current aggregate is not NULL, we need to advance it by the next
   *     value.
   *
   * If the update value is NULL, we can skip it.
   */

  PELOTON_ASSERT(agg_info.storage_idx < std::numeric_limits<uint32_t>::max());

  uint32_t storage_idx = agg_info.storage_idx;

  llvm::Value *null_byte_snapshot = null_bitmap.ByteFor(codegen, storage_idx);

  lang::If update_not_null(codegen, next.IsNotNull(codegen));
  {
    lang::If agg_is_null(codegen, null_bitmap.IsNull(codegen, storage_idx));
    {
      // Set initial
      codegen::Value initial = InitialAggValue(codegen, agg_info, &next);
      storage_.SetValue(codegen, space, storage_idx, initial, null_bitmap);
    }
    agg_is_null.ElseBlock();
    {
      // Perform proper merge
      AdvanceAgg(codegen, storage_, space, agg_info, next);
    }
    agg_is_null.EndIf();

    // Update NULL bitmap
    null_bitmap.MergeValues(agg_is_null, null_byte_snapshot);
  }
  update_not_null.EndIf();

  // Finalize value of NULL in bitmap
  null_bitmap.MergeValues(update_not_null, null_byte_snapshot);
}

void Aggregation::AdvanceValues(
    CodeGen &codegen, llvm::Value *aggs,
    const std::vector<codegen::Value> &next_vals) const {
  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, aggs);

  for (const auto &agg_info : aggregate_infos_) {
    // Skip derivative aggregates. Their values will be finalized later.
    if (agg_info.derived) {
      continue;
    }

    // Skip distinct aggregates. Their values will be merged in later.
    if (agg_info.distinct) {
      continue;
    }

    // Advance the current aggregate
    AdvanceValue(codegen, aggs, agg_info, next_vals[agg_info.source_idx],
                 null_bitmap);
  }

  // Write the final contents of the null bitmap
  null_bitmap.WriteBack(codegen);
}

void Aggregation::AdvanceDistinctValue(CodeGen &codegen, llvm::Value *aggs,
                                       uint32_t index,
                                       const codegen::Value &val) const {
  PELOTON_ASSERT(index < aggregate_infos_.size());

  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, aggs);

  for (const auto &agg_info : aggregate_infos_) {
    // Skip derivative aggregates. Their values will be finalized later.
    if (agg_info.derived) {
      continue;
    }

    // Skip unrelated aggregates
    if (agg_info.source_idx != index) {
      continue;
    }

    // Advance the current aggregate
    AdvanceValue(codegen, aggs, agg_info, val, null_bitmap);
  }

  null_bitmap.WriteBack(codegen);
}

void Aggregation::MergePartialAggregates(CodeGen &codegen,
                                         llvm::Value *dest_aggs,
                                         llvm::Value *src_aggs) const {
  UpdateableStorage::NullBitmap curr_null_bitmap(codegen, storage_, dest_aggs);
  UpdateableStorage::NullBitmap new_null_bitmap(codegen, storage_, src_aggs);

  for (const auto &agg_info : aggregate_infos_) {
    // Skip derivative aggregates. Their values will be finalized later.
    if (agg_info.derived) {
      continue;
    }

    // If the aggregate isn't NULL-able, use the fast path to merge
    if (!curr_null_bitmap.IsNullable(agg_info.storage_idx)) {
      MergePartialAgg(codegen, storage_, agg_info, dest_aggs, src_aggs);
      continue;
    }

    /*
     * The aggregate at this position is NULL-able. If the partial aggregate is
     * NULL, we needn't do anything. If the partial aggregate is not NULL, but
     * the current aggregate is NULL, we overwrite the current value with the
     * partial. If both are non-NULL, we do a proper merge.
     */

    uint32_t storage_idx = agg_info.storage_idx;

    llvm::Value *null_byte_snapshot =
        curr_null_bitmap.ByteFor(codegen, storage_idx);

    llvm::Value *partial_null = new_null_bitmap.IsNull(codegen, storage_idx);
    lang::If partial_not_null(codegen, codegen->CreateNot(partial_null));
    {
      llvm::Value *current_null = curr_null_bitmap.IsNull(codegen, storage_idx);
      lang::If curr_is_null(codegen, current_null);
      {
        auto partial =
            storage_.GetValueSkipNull(codegen, src_aggs, storage_idx);
        storage_.SetValue(codegen, dest_aggs, storage_idx, partial,
                          curr_null_bitmap);
      }
      curr_is_null.ElseBlock();
      {
        // Normal merge
        MergePartialAgg(codegen, storage_, agg_info, dest_aggs, src_aggs);
      }
      curr_is_null.EndIf();

      // Merge NULL value
      curr_null_bitmap.MergeValues(curr_is_null, null_byte_snapshot);
    }
    partial_not_null.EndIf();

    // Finalize value of NULL in bitmap
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

    const ExpressionType agg_type = agg_info.agg_type;
    switch (agg_type) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Neither COUNT(...) or COUNT(*) can ever return NULL, so no NULL-check
        final_val =
            storage_.GetValueSkipNull(codegen, space, agg_info.storage_idx);
        break;
      }
      case ExpressionType::AGGREGATE_SUM: {
        final_val = storage_.GetValue(codegen, space, agg_info.storage_idx,
                                      null_bitmap);
        if (agg_info.type != agg_info.output_type) {
          final_val = final_val.CastTo(codegen, agg_info.output_type);
        }
        break;
      }
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        final_val = storage_.GetValue(codegen, space, agg_info.storage_idx,
                                      null_bitmap);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // Collect the final values of the SUM and the COUNT components
        codegen::Value sum = all_vals[std::make_pair(
            agg_info.source_idx, ExpressionType::AGGREGATE_SUM)];

        codegen::Value count = all_vals[std::make_pair(
            agg_info.source_idx, ExpressionType::AGGREGATE_COUNT)];

        // Cast the values to DECIMAL
        const type::Type &decimal = agg_info.output_type;
        sum = sum.CastTo(codegen, decimal);
        count = count.CastTo(codegen, decimal);

        // Compute the average
        final_val = sum.Div(codegen, count, OnError::ReturnNull);
        break;
      }
      default: {
        throw Exception(StringUtil::Format(
            "Unexpected aggregate type '%s' when finalizing aggregator",
            ExpressionTypeToString(agg_type).c_str()));
      }
    }

    // Insert into global map
    all_vals.emplace(std::make_pair(agg_info.source_idx, agg_type), final_val);

    // If the aggregate isn't internal, push the value out
    if (!agg_info.internal) {
      final_vals.push_back(final_val);
    }
  }
}

}  // namespace codegen
}  // namespace peloton
