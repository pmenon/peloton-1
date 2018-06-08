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
#include "codegen/proxy/oa_hash_table_proxy.h"
#include "codegen/type/bigint_type.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/decimal_type.h"

namespace peloton {
namespace codegen {

// Configure/setup the aggregation class to handle the provided aggregate types
void Aggregation::Setup(
    CodeGen &codegen,
    const std::vector<planner::AggregatePlan::AggTerm> &aggregates,
    bool is_global) {
  is_global_ = is_global;

  for (uint32_t source_idx = 0; source_idx < aggregates.size(); source_idx++) {
    const auto &agg_term = aggregates[source_idx];
    switch (agg_term.aggtype) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        /*
         * COUNT(...)'s can never be NULL.
         */
        const type::Type count_type(type::BigInt::Instance(), false);
        uint32_t storage_pos = storage_.AddType(count_type);

        aggregate_infos_.emplace_back(
            AggregateInfo{.aggregate_type = agg_term.aggtype,
                          .source_index = source_idx,
                          .storage_indices = {{storage_pos}},
                          .is_distinct = agg_term.distinct});
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
            AggregateInfo{.aggregate_type = agg_term.aggtype,
                          .source_index = source_idx,
                          .storage_indices = {{storage_pos}},
                          .is_distinct = agg_term.distinct});
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
            AggregateInfo{.aggregate_type = agg_term.aggtype,
                          .source_index = source_idx,
                          .storage_indices = {{storage_pos}},
                          .is_distinct = agg_term.distinct});
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
        const type::Type count_type(type::BigInt::Instance(), false);
        auto sum_type = agg_term.expression->ResultType();
        if (IsGlobal()) {
          sum_type = sum_type.AsNullable();
        }

        uint32_t sum_storage_pos = storage_.AddType(sum_type);
        uint32_t count_storage_pos = storage_.AddType(count_type);

        aggregate_infos_.emplace_back(AggregateInfo{
            .aggregate_type = agg_term.aggtype,
            .source_index = source_idx,
            .storage_indices = {{sum_storage_pos, count_storage_pos}},
            .is_distinct = agg_term.distinct});
        break;
      }
      default: {
        throw Exception(StringUtil::Format(
            "Unexpected aggregate type '%s' when preparing aggregation",
            ExpressionTypeToString(agg_term.aggtype).c_str()));
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

// Create the initial values of all aggregates based on the the provided values
void Aggregation::CreateInitialValues(
    CodeGen &codegen, llvm::Value *space,
    const std::vector<codegen::Value> &initial) const {
  // Global aggregations should be calling CreateInitialGlobalValues(...)
  PELOTON_ASSERT(!IsGlobal());

  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);

  // Initialize bitmap to all NULLs
  null_bitmap.InitAllNull(codegen);

  // Iterate over the aggregations
  for (const auto &agg_info : aggregate_infos_) {
    const auto &input_val = initial[agg_info.source_index];

    switch (agg_info.aggregate_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX:
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Those aggregations consist of only one component
        DoInitializeValue(codegen, space, agg_info.aggregate_type,
                          agg_info.storage_indices[0], input_val, null_bitmap);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // AVG has to initialize both the SUM and the COUNT
        DoInitializeValue(codegen, space, ExpressionType::AGGREGATE_SUM,
                          agg_info.storage_indices[0], input_val, null_bitmap);
        DoInitializeValue(codegen, space, ExpressionType::AGGREGATE_COUNT,
                          agg_info.storage_indices[1], input_val, null_bitmap);
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

void Aggregation::DoInitializeValue(
    CodeGen &codegen, llvm::Value *space, ExpressionType type,
    uint32_t storage_index, const Value &initial,
    UpdateableStorage::NullBitmap &null_bitmap) const {
  switch (type) {
    case ExpressionType::AGGREGATE_SUM:
    case ExpressionType::AGGREGATE_MIN:
    case ExpressionType::AGGREGATE_MAX: {
      // For the above aggregations, the initial value is the attribute value
      storage_.SetValue(codegen, space, storage_index, initial, null_bitmap);
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
      codegen::Value initial_val{type::BigInt::Instance(), raw_initial};
      storage_.SetValueSkipNull(codegen, space, storage_index, initial_val);
      break;
    }
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      // The initial value for COUNT(*) is 1
      codegen::Value one{type::BigInt::Instance(), codegen.Const64(1)};
      storage_.SetValueSkipNull(codegen, space, storage_index, one);
      break;
    }
    default: {
      throw Exception(StringUtil::Format(
          "Unexpected aggregate type '%s' when creating initial values",
          ExpressionTypeToString(type).c_str()));
    }
  }
}

void Aggregation::DoAdvanceValue(CodeGen &codegen, llvm::Value *space,
                                 ExpressionType type, uint32_t storage_index,
                                 const codegen::Value &update) const {
  codegen::Value next;
  switch (type) {
    case ExpressionType::AGGREGATE_SUM: {
      auto curr = storage_.GetValueSkipNull(codegen, space, storage_index);
      next = curr.Add(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_MIN: {
      auto curr = storage_.GetValueSkipNull(codegen, space, storage_index);
      next = curr.Min(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_MAX: {
      auto curr = storage_.GetValueSkipNull(codegen, space, storage_index);
      next = curr.Max(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_COUNT: {
      auto curr = storage_.GetValueSkipNull(codegen, space, storage_index);

      // Convert the next update into 0 or 1 depending of if it is NULL
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
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      auto curr = storage_.GetValueSkipNull(codegen, space, storage_index);
      auto delta = codegen::Value(type::BigInt::Instance(), codegen.Const64(1));
      next = curr.Add(codegen, delta);
      break;
    }
    default: {
      throw Exception(StringUtil::Format(
          "Unexpected aggregate type '%s' when advancing aggregator",
          ExpressionTypeToString(type).c_str()));
    }
  }

  // Store the updated value in the appropriate slot
  storage_.SetValueSkipNull(codegen, space, storage_index, next);
}

void Aggregation::DoNullCheck(
    CodeGen &codegen, llvm::Value *space, ExpressionType type,
    uint32_t storage_index, const codegen::Value &update,
    UpdateableStorage::NullBitmap &null_bitmap) const {
  // This aggregate is NULL-able, we need to check if the update value is
  // NULL, and whether the current value of the aggregate is NULL.

  // There are two cases we handle:
  // (1). If neither the update value or the current aggregate value are
  //      NULL, we simple do the regular aggregation without NULL checking.
  // (2). If the update value is not NULL, but the current aggregate **is**
  //      NULL, then we just store this update value as if we're creating it
  //      for the first time.
  //
  // If either the update value or the current aggregate are NULL, we have
  // nothing to do.

  llvm::Value *update_not_null = update.IsNotNull(codegen);
  llvm::Value *agg_null = null_bitmap.IsNull(codegen, storage_index);

  // Fetch null byte so we can phi-resolve it after all the branches
  llvm::Value *null_byte_snapshot = null_bitmap.ByteFor(codegen, storage_index);

  lang::If valid_update(codegen, update_not_null);
  {
    lang::If agg_is_null(codegen, agg_null);
    {
      // (2)
      switch (type) {
        case ExpressionType::AGGREGATE_SUM:
        case ExpressionType::AGGREGATE_MIN:
        case ExpressionType::AGGREGATE_MAX: {
          storage_.SetValue(codegen, space, storage_index, update, null_bitmap);
          break;
        }
        case ExpressionType::AGGREGATE_COUNT: {
          codegen::Value one(type::BigInt::Instance(), codegen.Const64(1));
          storage_.SetValue(codegen, space, storage_index, one, null_bitmap);
          break;
        }
        default: { break; }
      }
    }
    agg_is_null.ElseBlock();
    {
      // (1)
      DoAdvanceValue(codegen, space, type, storage_index, update);
    }
    agg_is_null.EndIf();

    // Merge the null value
    null_bitmap.MergeValues(agg_is_null, null_byte_snapshot);
  }
  valid_update.EndIf();

  // Merge the null value
  null_bitmap.MergeValues(valid_update, null_byte_snapshot);
}

// Advance each of the aggregates stored in the provided storage space
void Aggregation::AdvanceValues(
    CodeGen &codegen, llvm::Value *space,
    const std::vector<codegen::Value> &next_vals) const {
  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);

  // Loop over all aggregates, advancing each
  for (const auto &aggregate_info : aggregate_infos_) {
    const Value &update = next_vals[aggregate_info.source_index];

    switch (aggregate_info.aggregate_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        // If the aggregate is not NULL-able, elide NULL check
        if (!null_bitmap.IsNullable(aggregate_info.storage_indices[0])) {
          DoAdvanceValue(codegen, space, aggregate_info.aggregate_type,
                         aggregate_info.storage_indices[0], update);
        } else {
          DoNullCheck(codegen, space, aggregate_info.aggregate_type,
                      aggregate_info.storage_indices[0], update, null_bitmap);
        }
        break;
      }
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // COUNT can't be nullable, so skip the null check
        DoAdvanceValue(codegen, space, ExpressionType::AGGREGATE_COUNT,
                       aggregate_info.storage_indices[0], update);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // If the SUM is not NULL-able, elide NULL check
        if (!null_bitmap.IsNullable(aggregate_info.storage_indices[0])) {
          DoAdvanceValue(codegen, space, ExpressionType::AGGREGATE_SUM,
                         aggregate_info.storage_indices[0], update);
        } else {
          DoNullCheck(codegen, space, ExpressionType::AGGREGATE_SUM,
                      aggregate_info.storage_indices[0], update, null_bitmap);
        }

        // COUNT can't be nullable, so skip the null check
        DoAdvanceValue(codegen, space, ExpressionType::AGGREGATE_COUNT,
                       aggregate_info.storage_indices[1], update);

        break;
      }
      default: {
        throw Exception(StringUtil::Format(
            "Unexpected aggregate type '%s' when advancing aggregator",
            ExpressionTypeToString(aggregate_info.aggregate_type).c_str()));
      }
    }
  }

  // Write the final contents of the null bitmap
  null_bitmap.WriteBack(codegen);
}

// This function will compute the final values of all aggregates stored in the
// provided storage space, populating the provided vector with these values.
void Aggregation::FinalizeValues(
    CodeGen &codegen, llvm::Value *space,
    std::vector<codegen::Value> &final_vals) const {
  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_, space);

  for (const auto &agg_info : aggregate_infos_) {
    ExpressionType agg_type = agg_info.aggregate_type;
    switch (agg_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        codegen::Value final_val = storage_.GetValue(
            codegen, space, agg_info.storage_indices[0], null_bitmap);

        // append final value to result vector
        final_vals.push_back(final_val);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // collect the final values of the SUM and the COUNT components
        codegen::Value sum = storage_.GetValue(
            codegen, space, agg_info.storage_indices[0], null_bitmap);

        PELOTON_ASSERT(!null_bitmap.IsNullable(agg_info.storage_indices[1]));
        codegen::Value count = storage_.GetValueSkipNull(
            codegen, space, agg_info.storage_indices[1]);

        // cast the values to DECIMAL
        codegen::Value sum_casted =
            sum.CastTo(codegen, type::Decimal::Instance());
        codegen::Value count_casted =
            count.CastTo(codegen, type::Decimal::Instance());

        // Compute the average
        codegen::Value final_val =
            sum_casted.Div(codegen, count_casted, OnError::ReturnNull);

        // append final value to result vector
        final_vals.push_back(final_val);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Neither COUNT(...) or COUNT(*) can ever return NULL, so no NULL-check
        codegen::Value final_val = storage_.GetValueSkipNull(
            codegen, space, agg_info.storage_indices[0]);

        // append final value to result vector
        final_vals.push_back(final_val);
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

void Aggregation::MergeValues(CodeGen &codegen, llvm::Value *curr_vals,
                              llvm::Value *new_vals) const {
  // TODO: Implement me
  (void)codegen;
  (void)curr_vals;
  (void)new_vals;
  throw NotImplementedException("Merging values not supported yet");
}

}  // namespace codegen
}  // namespace peloton
