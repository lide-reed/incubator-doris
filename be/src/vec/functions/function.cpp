// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/IFunction.cpp
// and modified by Doris

#include "vec/functions/function.h"

#include <algorithm>
#include <memory>
#include <numeric>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function_helpers.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
ColumnPtr wrap_in_nullable(const ColumnPtr& src, const Block& block, const ColumnNumbers& args,
                           uint32_t result, size_t input_rows_count) {
    ColumnPtr result_null_map_column;
    /// If result is already nullable.
    ColumnPtr src_not_nullable = src;
    MutableColumnPtr mutable_result_null_map_column;

    if (const auto* nullable = check_and_get_column<ColumnNullable>(*src)) {
        src_not_nullable = nullable->get_nested_column_ptr();
        result_null_map_column = nullable->get_null_map_column_ptr();
    }

    for (const auto& arg : args) {
        const ColumnWithTypeAndName& elem = block.get_by_position(arg);
        if (!elem.type->is_nullable() || is_column_const(*elem.column)) {
            continue;
        }

        if (const auto* nullable = assert_cast<const ColumnNullable*>(elem.column.get());
            nullable->has_null()) {
            const ColumnPtr& null_map_column = nullable->get_null_map_column_ptr();
            if (!result_null_map_column) { // NOLINT(bugprone-use-after-move)
                result_null_map_column = null_map_column->clone_resized(input_rows_count);
                continue;
            }

            if (!mutable_result_null_map_column) {
                mutable_result_null_map_column =
                        std::move(result_null_map_column)->assume_mutable();
            }

            NullMap& result_null_map =
                    assert_cast<ColumnUInt8&>(*mutable_result_null_map_column).get_data();
            const NullMap& src_null_map =
                    assert_cast<const ColumnUInt8&>(*null_map_column).get_data();

            VectorizedUtils::update_null_map(result_null_map, src_null_map);
        }
    }

    if (!result_null_map_column) {
        if (is_column_const(*src)) {
            return ColumnConst::create(
                    make_nullable(assert_cast<const ColumnConst&>(*src).get_data_column_ptr(),
                                  false),
                    input_rows_count);
        }
        return ColumnNullable::create(src, ColumnUInt8::create(input_rows_count, 0));
    }

    return ColumnNullable::create(src_not_nullable, result_null_map_column);
}

bool have_null_column(const Block& block, const ColumnNumbers& args) {
    return std::ranges::any_of(args, [&block](const auto& elem) {
        return block.get_by_position(elem).type->is_nullable();
    });
}

bool have_null_column(const ColumnsWithTypeAndName& args) {
    return std::ranges::any_of(args, [](const auto& elem) { return elem.type->is_nullable(); });
}

inline Status PreparedFunctionImpl::_execute_skipped_constant_deal(
        FunctionContext* context, Block& block, const ColumnNumbers& args, uint32_t result,
        size_t input_rows_count, bool dry_run) const {
    bool executed = false;
    RETURN_IF_ERROR(default_implementation_for_nulls(context, block, args, result, input_rows_count,
                                                     dry_run, &executed));
    if (executed) {
        return Status::OK();
    }

    if (dry_run) {
        return execute_impl_dry_run(context, block, args, result, input_rows_count);
    } else {
        return execute_impl(context, block, args, result, input_rows_count);
    }
}

Status PreparedFunctionImpl::default_implementation_for_constant_arguments(
        FunctionContext* context, Block& block, const ColumnNumbers& args, uint32_t result,
        size_t input_rows_count, bool dry_run, bool* executed) const {
    *executed = false;
    ColumnNumbers args_expect_const = get_arguments_that_are_always_constant();

    // Check that these arguments are really constant.
    for (auto arg_num : args_expect_const) {
        if (arg_num < args.size() &&
            !is_column_const(*block.get_by_position(args[arg_num]).column)) {
            return Status::InvalidArgument("Argument at index {} for function {} must be constant",
                                           arg_num, get_name());
        }
    }

    if (args.empty() || !use_default_implementation_for_constants() ||
        !VectorizedUtils::all_arguments_are_constant(block, args)) {
        return Status::OK();
    }

    // now all columns are const.
    Block temporary_block;

    int arguments_size = (int)args.size();
    for (size_t arg_num = 0; arg_num < arguments_size; ++arg_num) {
        const ColumnWithTypeAndName& column = block.get_by_position(args[arg_num]);
        // Columns in const_list --> column_const,    others --> nested_column
        // that's because some functions supposes some specific columns always constant.
        // If we unpack it, there will be unnecessary cost of virtual judge.
        if (args_expect_const.end() !=
            std::find(args_expect_const.begin(), args_expect_const.end(), arg_num)) {
            temporary_block.insert({column.column, column.type, column.name});
        } else {
            temporary_block.insert(
                    {assert_cast<const ColumnConst*>(column.column.get())->get_data_column_ptr(),
                     column.type, column.name});
        }
    }

    temporary_block.insert(block.get_by_position(result));

    ColumnNumbers temporary_argument_numbers(arguments_size);
    for (int i = 0; i < arguments_size; ++i) {
        temporary_argument_numbers[i] = i;
    }

    RETURN_IF_ERROR(_execute_skipped_constant_deal(context, temporary_block,
                                                   temporary_argument_numbers, arguments_size,
                                                   temporary_block.rows(), dry_run));

    ColumnPtr result_column;
    /// extremely rare case, when we have function with completely const arguments
    /// but some of them produced by non is_deterministic function
    if (temporary_block.get_by_position(arguments_size).column->size() > 1) {
        result_column = temporary_block.get_by_position(arguments_size).column->clone_resized(1);
    } else {
        result_column = temporary_block.get_by_position(arguments_size).column;
    }
    // We shuold handle the case where the result column is also a ColumnConst.
    block.get_by_position(result).column = ColumnConst::create(result_column, input_rows_count);
    *executed = true;
    return Status::OK();
}

Status PreparedFunctionImpl::default_implementation_for_nulls(
        FunctionContext* context, Block& block, const ColumnNumbers& args, uint32_t result,
        size_t input_rows_count, bool dry_run, bool* executed) const {
    *executed = false;
    if (args.empty() || !use_default_implementation_for_nulls()) {
        return Status::OK();
    }

    if (std::ranges::any_of(args, [&block](const auto& elem) {
            return block.get_by_position(elem).column->only_null();
        })) {
        block.get_by_position(result).column =
                block.get_by_position(result).type->create_column_const(input_rows_count, Field());
        *executed = true;
        return Status::OK();
    }

    if (have_null_column(block, args)) {
        bool need_to_default = need_replace_null_data_to_default();
        if (context) {
            need_to_default &= context->check_overflow_for_decimal();
        }
        // extract nested column from nulls
        ColumnNumbers new_args;
        for (auto arg : args) {
            new_args.push_back(block.columns());
            block.insert(block.get_by_position(arg).get_nested(need_to_default));
            DCHECK(!block.get_by_position(new_args.back()).column->is_nullable());
        }
        RETURN_IF_ERROR(execute_without_low_cardinality_columns(context, block, new_args, result,
                                                                block.rows(), dry_run));
        // After run with nested, wrap them in null. Before this, block.get_by_position(result).type
        // is not compatible with get_by_position(result).column
        block.get_by_position(result).column = wrap_in_nullable(
                block.get_by_position(result).column, block, args, result, input_rows_count);

        while (!new_args.empty()) {
            block.erase(new_args.back());
            new_args.pop_back();
        }
        *executed = true;
        return Status::OK();
    }
    return Status::OK();
}

Status PreparedFunctionImpl::execute_without_low_cardinality_columns(
        FunctionContext* context, Block& block, const ColumnNumbers& args, uint32_t result,
        size_t input_rows_count, bool dry_run) const {
    bool executed = false;

    RETURN_IF_ERROR(default_implementation_for_constant_arguments(
            context, block, args, result, input_rows_count, dry_run, &executed));
    if (executed) {
        return Status::OK();
    }

    return _execute_skipped_constant_deal(context, block, args, result, input_rows_count, dry_run);
}

Status PreparedFunctionImpl::execute(FunctionContext* context, Block& block,
                                     const ColumnNumbers& args, uint32_t result,
                                     size_t input_rows_count, bool dry_run) const {
    return execute_without_low_cardinality_columns(context, block, args, result, input_rows_count,
                                                   dry_run);
}

void FunctionBuilderImpl::check_number_of_arguments(size_t number_of_arguments) const {
    if (is_variadic()) {
        return;
    }

    size_t expected_number_of_arguments = get_number_of_arguments();

    CHECK_EQ(number_of_arguments, expected_number_of_arguments) << fmt::format(
            "Number of arguments for function {} doesn't match: passed {} , should be {}",
            get_name(), number_of_arguments, expected_number_of_arguments);
}

DataTypePtr FunctionBuilderImpl::get_return_type(const ColumnsWithTypeAndName& arguments) const {
    check_number_of_arguments(arguments.size());

    if (!arguments.empty() && use_default_implementation_for_nulls()) {
        if (have_null_column(arguments)) {
            ColumnNumbers numbers(arguments.size());
            std::iota(numbers.begin(), numbers.end(), 0);
            auto [nested_block, _] =
                    create_block_with_nested_columns(Block(arguments), numbers, false);
            auto return_type = get_return_type_impl(
                    ColumnsWithTypeAndName(nested_block.begin(), nested_block.end()));
            return make_nullable(return_type);
        }
    }

    return get_return_type_impl(arguments);
}

bool FunctionBuilderImpl::is_date_or_datetime_or_decimal(
        const DataTypePtr& return_type, const DataTypePtr& func_return_type) const {
    return (is_date_or_datetime(return_type->get_primitive_type()) &&
            is_date_or_datetime(func_return_type->get_primitive_type())) ||
           (is_date_v2_or_datetime_v2(return_type->get_primitive_type()) &&
            is_date_v2_or_datetime_v2(func_return_type->get_primitive_type())) ||
           // For some date functions such as str_to_date(string, string), return_type will
           // be datetimev2 if users enable datev2 but get_return_type(arguments) will still
           // return datetime. We need keep backward compatibility here.
           (is_date_v2_or_datetime_v2(return_type->get_primitive_type()) &&
            is_date_or_datetime(func_return_type->get_primitive_type())) ||
           (is_date_or_datetime(return_type->get_primitive_type()) &&
            is_date_v2_or_datetime_v2(func_return_type->get_primitive_type())) ||
           (is_decimal(return_type->get_primitive_type()) &&
            is_decimal(func_return_type->get_primitive_type()));
}

bool FunctionBuilderImpl::is_array_nested_type_date_or_datetime_or_decimal(
        const DataTypePtr& return_type, const DataTypePtr& func_return_type) const {
    auto return_type_ptr = return_type->is_nullable()
                                   ? ((DataTypeNullable*)return_type.get())->get_nested_type()
                                   : return_type;
    auto func_return_type_ptr =
            func_return_type->is_nullable()
                    ? ((DataTypeNullable*)func_return_type.get())->get_nested_type()
                    : func_return_type;
    if (!(return_type_ptr->get_primitive_type() == TYPE_ARRAY &&
          func_return_type_ptr->get_primitive_type() == TYPE_ARRAY)) {
        return false;
    }
    auto nested_nullable_return_type_ptr =
            (assert_cast<const DataTypeArray*>(return_type_ptr.get()))->get_nested_type();
    auto nested_nullable_func_return_type =
            (assert_cast<const DataTypeArray*>(func_return_type_ptr.get()))->get_nested_type();
    // There must be nullable inside array type.
    if (nested_nullable_return_type_ptr->is_nullable() &&
        nested_nullable_func_return_type->is_nullable()) {
        auto nested_return_type_ptr =
                ((DataTypeNullable*)(nested_nullable_return_type_ptr.get()))->get_nested_type();
        auto nested_func_return_type_ptr =
                ((DataTypeNullable*)(nested_nullable_func_return_type.get()))->get_nested_type();
        return is_date_or_datetime_or_decimal(nested_return_type_ptr, nested_func_return_type_ptr);
    }
    return false;
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
