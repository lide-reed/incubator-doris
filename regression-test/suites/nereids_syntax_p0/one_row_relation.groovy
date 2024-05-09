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

suite("one_row_relation") {
    // enable nereids and vectorized engine
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    test {
        sql "select 100, 'abc', substring('abc', 1, 2), substring(substring('abcdefg', 4, 3), 1, 2), null"
        result([[100, "abc", "ab", "de", null]])
    }

    test {
        sql """select * from (
            select 100, 'abc', substring('abc', 1, 2), substring(substring('abcdefg', 4, 3), 1, 2), null
        )a"""
        result([[100, "abc", "ab", "de", null]])
    }

    qt_string1 """ select 'A''B', 'A''''B', 'A\\'\\'B', ''; """
    qt_string2 """ select "A""B", "A\\"\\"B", "";  """
    qt_string3 """ select 'A""B', 'A\\"\\"B';  """
    qt_string4 """ select "A''B", "A\\'\\'B";  """
}