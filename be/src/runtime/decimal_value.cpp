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

#include "runtime/decimal_value.h"
#include "util/string_parser.hpp"

#include <algorithm>
#include <iostream>
#include <utility>

namespace doris {

const char* DecimalValue::_s_llvm_class_name = "class.doris::DecimalValue";

// x>=0 && y>=0
static int do_add(__int128 x, __int128 y, __int128* result) {
    int error = E_DEC_OK;
    if (DecimalValue::MAX_DECIMAL_VALUE - x >= y) {
        *result = x + y;
    } else {
        *result = DecimalValue::MAX_DECIMAL_VALUE;
        error = E_DEC_OVERFLOW;
    }
    return error;
}

// x>=0 && y>=0
static int do_sub(__int128 x, __int128 y, __int128* result) {
    int error = E_DEC_OK;
    *result = x - y;
    return error;
}

// clear leading zero for int128
static int clz128(unsigned __int128 v) {
  if (v == 0) return 128;
  unsigned __int128 shifted = v >> 64;
  if (shifted != 0) {
    return __builtin_clzll(shifted);
  } else {
    return __builtin_clzll(v) + 64;
  }
}

static int do_mul(__int128 x, __int128 y, __int128* result) {
    int error = E_DEC_OK;
    if (x == 0 || y == 0) {
        *result = 0;
        return error;
    }

    // count leading zero
    int clz = clz128(abs(x)) + clz128(abs(y)); 
    if (clz <= 128 && (abs(x) > DecimalValue::MAX_DECIMAL_VALUE / abs(y))) {
        *result = DecimalValue::MAX_DECIMAL_VALUE;
        error = E_DEC_OVERFLOW;
        return error;
    }

    *result = x * y / DecimalValue::MULTIPLIER;

    // overflow
    if (*result * DecimalValue::MULTIPLIER / y != x) {
        *result = DecimalValue::MAX_DECIMAL_VALUE;
        error = E_DEC_OVERFLOW;
    }
    return error;
}

static int do_div(__int128 x, __int128 y, __int128* result) {
    int error = E_DEC_OK;
    if (y == 0) {
        *result = 0;
        return E_DEC_DIV_ZERO;
    }

    if (x == 0) {
        *result = 0;
        return error;
    }

    *result = x * DecimalValue::MULTIPLIER / y;

    // overflow
    if (*result * y != x * DecimalValue::MULTIPLIER) {
        *result = DecimalValue::MAX_DECIMAL_VALUE;
        error = E_DEC_OVERFLOW;
    }
    return error;
}

static int do_mod(__int128 x, __int128 y, __int128* result) {
    int error = E_DEC_OK;
    if (y == 0) {
        *result = 0;
        return E_DEC_DIV_ZERO;
    }

    if (x == 0) {
        *result = 0;
        return error;
    }

    *result = x % y;
    return error;
}

DecimalValue operator+(const DecimalValue& v1, const DecimalValue& v2) {
    __int128 result;
    __int128 x = v1.value();
    __int128 y = v2.value();
    DecimalValue value;
    if (x == 0) {
       result = y;
    } else if (y == 0) {
       result = x;
    } else if (x > 0) {
        if (y > 0) {
            do_add(x, y, &result);
        } else {
            do_sub(x, -y, &result);
        }
    } else { // x < 0
        if (y > 0) {
            do_sub(y, -x, &result);
        } else {
            do_add(-x, -y, &result);
            result = -result;
        }
    }

    value.set_value(result);
    return value;
}

DecimalValue operator-(const DecimalValue& v1, const DecimalValue& v2) {
    __int128 result;
    __int128 x = v1.value();
    __int128 y = v2.value();
    DecimalValue value;
    if (x == 0) {
       result = -y;
    } else if (y == 0) {
       result = x;
    } else if (x > 0) {
        if (y > 0) {
            do_sub(x, y, &result);
        } else {
            do_add(x, -y, &result);
        }
    } else { // x < 0
        if (y > 0) {
            do_add(-x, y, &result);
            result = -result;
        } else {
            do_sub(-x, -y, &result);
            result = -result;
        }
    }

    value.set_value(result);
    return value;
}

DecimalValue operator*(const DecimalValue& v1, const DecimalValue& v2){
    __int128 result;
    __int128 x = v1.value();
    __int128 y = v2.value();
    DecimalValue value;

    do_mul(x, y, &result);

    value.set_value(result);
    return value;
}

DecimalValue operator/(const DecimalValue& v1, const DecimalValue& v2){
    __int128 result;
    __int128 x = v1.value();
    __int128 y = v2.value();
    DecimalValue value;

    do_div(x, y, &result);

    value.set_value(result);
    return value;
}

DecimalValue operator%(const DecimalValue& v1, const DecimalValue& v2){
    __int128 result;
    __int128 x = v1.value();
    __int128 y = v2.value();
    DecimalValue value;

    do_mod(x, y, &result);

    value.set_value(result);
    return value;
}

std::ostream& operator<<(std::ostream& os, DecimalValue const& decimal_value) {
    return os << decimal_value.to_string();
}

std::istream& operator>>(std::istream& ism, DecimalValue& decimal_value) {
    std::string str_buff;
    ism >> str_buff;
    decimal_value.parse_from_str(str_buff.c_str(), str_buff.size());
    return ism;
}

DecimalValue operator-(const DecimalValue& v) {
    DecimalValue result;
    result.set_value(-v.value());
    return result;
}

DecimalValue& DecimalValue::operator+=(const DecimalValue& other) {
    *this = *this + other;
    return *this;
}

int DecimalValue::parse_from_str(const char* decimal_str, int32_t length) {
    int32_t error = E_DEC_OK;
    StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
    if (result == StringParser::PARSE_SUCCESS) { 
        _value = StringParser::string_to_decimal(decimal_str, length, 
                 PRECISION, SCALE, &result);
    } else {
       set_to_zero();
       error = E_DEC_BAD_NUM;
    }
    return error;
}

std::string DecimalValue::to_string(int round_scale) const {
  if (_value == 0) return std::string(1, '0');

  int last_char_idx = PRECISION + 2 + (_value < 0);  
  std::string str = std::string(last_char_idx, '0');

  __int128 remaining_value = _value;
  int first_digit_idx = 0;
  if (_value < 0) {
      remaining_value = -_value;
      first_digit_idx = 1;
  }

  int remaining_scale = SCALE;
  do {
      str[--last_char_idx] = (remaining_value % 10) + '0'; 
      remaining_value /= 10;
  } while (--remaining_scale > 0);
  str[--last_char_idx] = '.';

  do {
      str[--last_char_idx] = (remaining_value % 10) + '0';
      remaining_value /= 10;
      if (remaining_value == 0) {
          if (last_char_idx > first_digit_idx) str.erase(0, last_char_idx - first_digit_idx);
          break;
      }
  } while (last_char_idx > first_digit_idx);

  if (_value < 0) str[0] = '-';

  // right trim and round
  int scale = 0;
  for(scale = 0; scale < SCALE; scale++) {
      if (str[str.size() - scale - 1] != '0') break;
  }
  if (scale == SCALE) scale++; //integer, trim .
  if (round_scale >= 0 && round_scale <= SCALE) {
      scale = std::max(scale, SCALE - round_scale);
  }
  if (scale > 1) str.erase(str.size() - scale, str.size() - 1);

  return str;
}

std::string DecimalValue::to_string() const {
    return to_string(-1);
}

// NOTE: only change abstract value, do not change sign
void DecimalValue::to_max_decimal(int32_t precision, int32_t scale) {
   bool is_negtive = (_value < 0);
   static const int64_t INT_MAX_VALUE[PRECISION] = {
        9ll, 
        99ll, 
        999ll, 
        9999ll, 
        99999ll,
        999999ll,
        9999999ll, 
        99999999ll, 
        999999999ll, 
        9999999999ll,
        99999999999ll, 
        999999999999ll, 
        9999999999999ll, 
        99999999999999ll, 
        999999999999999ll, 
        9999999999999999ll, 
        99999999999999999ll, 
        999999999999999999ll
   };
   static const int32_t FRAC_MAX_VALUE[SCALE] = { 
        900000000, 
        990000000, 
        999000000,
        999900000, 
        999990000, 
        999999000,
        999999900, 
        999999990, 
        999999999
   };

   if (precision <= 0 || scale < 0) return;

   // normalize precision and scale
   if (precision > PRECISION) precision = PRECISION;
   if (scale > SCALE) scale = SCALE;
   if (precision - scale > PRECISION - SCALE) {
       precision = PRECISION - SCALE + scale;
   }
   
   int64_t int_value = INT_MAX_VALUE[precision - scale - 1];
   int64_t frac_value = FRAC_MAX_VALUE[scale - 1];
   _value = static_cast<__int128>(int_value) * DecimalValue::MULTIPLIER + frac_value;
   if (is_negtive) _value = -_value;
}

std::size_t hash_value(DecimalValue const& value) {
    return value.hash(0);
}

int DecimalValue::round(DecimalValue *to, int rounding_scale, DecimalRoundMode op) {
    int32_t error = E_DEC_OK;
    __int128 result;
    
    if (rounding_scale >= SCALE) return error;
    if (rounding_scale < -(PRECISION - SCALE)) return 0;

    __int128 base = get_scale_base(SCALE - rounding_scale);
    result = _value / base;

    int one = _value > 0 ? 1 : -1;
    __int128 remainder = _value % base;
    switch (op) {
        case HALF_UP:
        case HALF_EVEN:
            if (abs(remainder) >= (base >> 1)) {
                result = (result + one) * base;
            } else {
                result = result * base;
            }
            break;
        case CEILING:
            if (remainder > 0 && _value > 0) {
                result = (result + one) * base;
            } else {
                result = result * base;
            }
            break;
        case FLOOR:
            if (remainder < 0 && _value < 0) {
                result = (result + one) * base;
            } else {
                result = result * base;
            }
            break;
        case TRUNCATE:
            result = result * base;
            break;
        default:
            break;
    }

    to->set_value(result);
    return error;
}

} // end namespace doris
