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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeDateTime.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>

#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <memory>
#include <string>

#include "runtime/define_primitive_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number_base.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde/data_type_time_serde.h"

namespace doris::vectorized {
class BufferWritable;
class IColumn;

class DataTypeTimeV2 final : public DataTypeNumberBase<PrimitiveType::TYPE_TIMEV2> {
public:
    DataTypeTimeV2(int scale = 0) : _scale(scale) {
        if (UNLIKELY(scale > 6)) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Scale {} is out of bounds", scale);
        }
        if (scale == -1) {
            _scale = 0;
        }
    }
    bool equals(const IDataType& rhs) const override;

    std::string to_string(const IColumn& column, size_t row_num) const override;
    std::string to_string(double int_val) const;

    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    void to_string_batch(const IColumn& column, ColumnString& column_to) const final {
        DataTypeNumberBase<PrimitiveType::TYPE_TIMEV2>::template to_string_batch_impl<
                DataTypeTimeV2>(column, column_to);
    }

    size_t number_length() const;
    void push_number(ColumnString::Chars& chars, const Float64& num) const;
    MutableColumnPtr create_column() const override;

    void to_pb_column_meta(PColumnMeta* col_meta) const override;
    using SerDeType = DataTypeTimeV2SerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(_scale, nesting_level);
    };
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_TIMEV2; }
    const std::string get_family_name() const override { return "timev2"; }
    UInt32 get_scale() const override { return _scale; }

    Field get_field(const TExprNode& node) const override;

private:
    UInt32 _scale;
};
} // namespace doris::vectorized
