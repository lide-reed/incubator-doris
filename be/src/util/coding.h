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
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>

#include "gutil/endian.h"

namespace doris {

// TODO(zc): add encode big endian later when we need it
// use big endian when we have order requirement.
// little endian is more efficient when we use X86 CPU, so
// when we have no order needs, we prefer little endian encoding
inline void encode_fixed8(uint8_t* buf, uint8_t val) {
    *buf = val;
}

inline void encode_fixed16_le(uint8_t* buf, uint16_t val) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    memcpy(buf, &val, sizeof(val));
#else
    uint16_t res = bswap_16(val);
    memcpy(buf, &res, sizeof(res));
#endif
}

inline void encode_fixed32_le(uint8_t* buf, uint32_t val) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    memcpy(buf, &val, sizeof(val));
#else
    uint32_t res = bswap_32(val);
    memcpy(buf, &res, sizeof(res));
#endif
}

inline void encode_fixed64_le(uint8_t* buf, uint64_t val) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    memcpy(buf, &val, sizeof(val));
#else
    uint64_t res = gbswap_64(val);
    memcpy(buf, &res, sizeof(res));
#endif
}

inline uint8_t decode_fixed8(const uint8_t* buf) {
    return *buf;
}

inline uint16_t decode_fixed16_le(const uint8_t* buf) {
    uint16_t res;
    memcpy(&res, buf, sizeof(res));
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return res;
#else
    return bswap_16(res);
#endif
}

inline uint32_t decode_fixed32_le(const uint8_t* buf) {
    uint32_t res;
    memcpy(&res, buf, sizeof(res));
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return res;
#else
    return bswap_32(res);
#endif
}

inline uint64_t decode_fixed64_le(const uint8_t* buf) {
    uint64_t res;
    memcpy(&res, buf, sizeof(res));
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return res;
#else
    return gbswap_64(res);
#endif
}

inline void put_fixed32_le(std::string* dst, uint32_t val) {
    uint8_t buf[sizeof(val)];
    encode_fixed32_le(buf, val);
    dst->append((char*)buf, sizeof(buf));
}

inline void put_fixed64_le(std::string* dst, uint64_t val) {
    uint8_t buf[sizeof(val)];
    encode_fixed64_le(buf, val);
    dst->append((char*)buf, sizeof(buf));
}

extern uint8_t* encode_varint32(uint8_t* dst, uint32_t value);
extern uint8_t* encode_varint64(uint8_t* dst, uint64_t value);

inline uint8_t* encode_varint64(uint8_t* dst, uint64_t v) {
    static const unsigned int B = 128;
    while (v >= B) {
        *(dst++) = (v & (B - 1)) | B;
        v >>= 7;
    }
    *(dst++) = static_cast<unsigned char>(v);
    return dst;
}

extern const uint8_t* decode_varint32_ptr_fallback(
    const uint8_t* p, const uint8_t* limit, uint32_t* value);

inline const uint8_t* decode_varint32_ptr(
        const uint8_t* ptr, const uint8_t* limit, uint32_t* value) {
    if (ptr < limit) {
        uint32_t result = *ptr;
        if ((result & 128) == 0) {
            *value = result;
            return ptr + 1;
        }
    }
    return decode_varint32_ptr_fallback(ptr, limit, value);
}

extern const uint8_t* decode_varint64_ptr(const uint8_t* p, const uint8_t* limit, uint64_t* value);

inline void put_varint32(std::string* dst, uint32_t v) {
    uint8_t buf[5];
    uint8_t* ptr = encode_varint32(buf, v);
    dst->append((char*)buf, static_cast<size_t>(ptr - buf));
}

inline void put_varint64(std::string* dst, uint64_t v) {
    uint8_t buf[10];
    uint8_t* ptr = encode_varint64(buf, v);
    dst->append((char*)buf, static_cast<size_t>(ptr - buf));
}

inline void put_varint64_varint32(std::string* dst, uint64_t v1, uint32_t v2) {
    uint8_t buf[15];
    uint8_t* ptr = encode_varint64(buf, v1);
    ptr = encode_varint32(ptr, v2);
    dst->append((char*)buf, static_cast<size_t>(ptr - buf));
}

}
