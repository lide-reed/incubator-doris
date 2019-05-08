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

package org.apache.doris.analysis;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class SelectStmtTest {
    @Test
    public void testGroupingSets() {
        List<ArrayList<Expr>> groupingExprsList = new ArrayList<>();
        String[][] colsLists = {
                {"k3", "k1"},
                {"k2", "k3", "k2"},
                {"k1", "k3"},
                {"k4"},
                {"k1", "k2", "k3", "k4"}
        };

        for(String[] colsList: colsLists) {
            ArrayList<Expr> exprList = new ArrayList<>();
            for (String col : colsList) {
                exprList.add(new SlotRef(new TableName("testdb", "t"), col));
            }
            groupingExprsList.add(exprList);
        }

        GroupByClause groupByClause = new GroupByClause(groupingExprsList, GroupByClause.GroupingType.GROUPING_SETS);
        List<BitSet> bitSetList = groupByClause.getGroupingIDBitSetList();

        String[] answer = {"{1, 3}", "{0, 3}", "{2}"};
        Set<String> answerSet = new HashSet<String>(Arrays.asList(answer));
        Set<String> resultSet = new HashSet<>();
        for (BitSet aBitSetList : bitSetList) {
            String s = aBitSetList.toString();
            resultSet.add(s);
        }

        Assert.assertEquals(answerSet, resultSet);
    }

    @Test
    public void testRollUp() {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k2", "k3", "k4", "k3"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName("testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause = new GroupByClause(groupingExprs, GroupByClause.GroupingType.ROLLUP);
        List<BitSet> bitSetList = groupByClause.getGroupingIDBitSetList();

        String[] answer = {"{}", "{0}", "{0, 1}"};
        Set<String> answerSet = new HashSet<String>(Arrays.asList(answer));
        Set<String> resultSet = new HashSet<>();
        for (BitSet aBitSetList : bitSetList) {
            String s = aBitSetList.toString();
            resultSet.add(s);
        }

        Assert.assertEquals(answerSet, resultSet);
    }

    @Test
    public void testCube() {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k1", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName("testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause = new GroupByClause(groupingExprs, GroupByClause.GroupingType.CUBE);
        List<BitSet> bitSetList = groupByClause.getGroupingIDBitSetList();

        String[] answer = {"{}", "{1}", "{0}", "{0, 1}", "{2}", "{1, 2}", "{0, 2}"};
        Set<String> answerSet = new HashSet<String>(Arrays.asList(answer));
        Set<String> resultSet = new HashSet<>();
        for (BitSet aBitSetList : bitSetList) {
            String s = aBitSetList.toString();
            resultSet.add(s);
        }

        Assert.assertEquals(answerSet, resultSet);
    }

    @Test
    public void testGroupBy() {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k2", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName("testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause = new GroupByClause(groupingExprs, GroupByClause.GroupingType.GROUP_BY);
        List<BitSet> bitSetList = groupByClause.getGroupingIDBitSetList();

        Assert.assertEquals(bitSetList, null);
        Assert.assertEquals(groupByClause.getGroupingExprs(), groupingExprs);
    }
}
