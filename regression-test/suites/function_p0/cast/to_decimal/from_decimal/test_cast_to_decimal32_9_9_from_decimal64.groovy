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


suite("test_cast_to_decimal32_9_9_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_0_0_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_0_0_nullable(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_0_0_nullable values (0, "0")
      ,(1, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_0_0_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_0_0_not_nullable(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_0_0_not_nullable values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_0_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_1_1_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_1_1_nullable(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_1_1_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9")
      ,(4, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_1_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_1_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_1_1_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_1_1_not_nullable(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_1_1_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_1_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_1_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_5_2_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_5_2_nullable(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_5_2_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.09999"),(4, "0.90000"),(5, "0.90001"),(6, "0.99998"),(7, "0.99999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_5_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_5_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_5_2_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_5_2_not_nullable(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_5_2_not_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.09999"),(4, "0.90000"),(5, "0.90001"),(6, "0.99998"),(7, "0.99999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_5_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_5_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_9_3_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_9_3_nullable(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_9_3_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_9_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_9_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_9_3_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_9_3_not_nullable(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_9_3_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_9_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_9_3_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_10_4_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_10_4_nullable(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_10_4_nullable values (0, "0.0000000000"),(1, "0.0000000001"),(2, "0.0000000009"),(3, "0.0999999999"),(4, "0.9000000000"),(5, "0.9000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_10_4_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_10_4_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_10_4_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_10_4_not_nullable(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_10_4_not_nullable values (0, "0.0000000000"),(1, "0.0000000001"),(2, "0.0000000009"),(3, "0.0999999999"),(4, "0.9000000000"),(5, "0.9000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_10_4_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_10_4_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_17_0_5_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_17_0_5_nullable(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_17_0_5_nullable values (0, "0")
      ,(1, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_0_5_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_0_5_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_17_0_5_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_17_0_5_not_nullable(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_17_0_5_not_nullable values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_0_5_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_0_5_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_17_1_6_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_17_1_6_nullable(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_17_1_6_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9")
      ,(4, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_1_6_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_1_6_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_17_1_6_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_17_1_6_not_nullable(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_17_1_6_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_1_6_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_1_6_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_17_8_7_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_17_8_7_nullable(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_17_8_7_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_8_7_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_8_7_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_17_8_7_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_17_8_7_not_nullable(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_17_8_7_not_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_8_7_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_8_7_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_17_16_8_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_17_16_8_nullable(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_17_16_8_nullable values (0, "0.0000000000000000"),(1, "0.0000000000000001"),(2, "0.0000000000000009"),(3, "0.0999999999999999"),(4, "0.9000000000000000"),(5, "0.9000000000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_16_8_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_16_8_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_17_16_8_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_17_16_8_not_nullable(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_17_16_8_not_nullable values (0, "0.0000000000000000"),(1, "0.0000000000000001"),(2, "0.0000000000000009"),(3, "0.0999999999999999"),(4, "0.9000000000000000"),(5, "0.9000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_16_8_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_16_8_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_17_17_9_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_17_17_9_nullable(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_17_17_9_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_17_9_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_17_9_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_17_17_9_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_17_17_9_not_nullable(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_17_17_9_not_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_17_9_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_17_17_9_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_0_10_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_0_10_nullable(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_0_10_nullable values (0, "0")
      ,(1, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_0_10_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_0_10_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_0_10_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_0_10_not_nullable(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_0_10_not_nullable values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_0_10_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_0_10_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_1_11_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_1_11_nullable(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_1_11_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9")
      ,(4, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_1_11_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_1_11_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_1_11_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_1_11_not_nullable(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_1_11_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_1_11_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_1_11_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_9_12_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_9_12_nullable(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_9_12_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_9_12_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_9_12_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_9_12_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_9_12_not_nullable(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_9_12_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_9_12_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_9_12_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_17_13_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_17_13_nullable(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_17_13_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_17_13_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_17_13_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_17_13_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_17_13_not_nullable(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_17_13_not_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_17_13_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_17_13_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_18_14_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_18_14_nullable(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_18_14_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_18_14_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_18_14_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_18_14_not_nullable;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_18_14_not_nullable(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_18_14_not_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_18_14_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_18_14_not_nullable order by 1;'

}