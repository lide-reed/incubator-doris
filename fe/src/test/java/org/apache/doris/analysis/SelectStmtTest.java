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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

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

        SelectStmt selectStmt = new SelectStmt(null, null, null, groupingExprsList, null, null, null);
        List<BitSet> bitSetList = selectStmt.getGroupingSetList();

        String[] result = {"{1, 3}", "{0, 3}", "{2}"};

        for(int i = 0; i < bitSetList.size(); i++) {
            String s = bitSetList.get(i).toString();
            Assert.assertEquals(result[i], s);
        }
    }

    @Test
    public void testRollUp() {
        List<ArrayList<Expr>> groupingExprsList = new ArrayList<>();
        String[] cols = {"k2", "k3", "k4"};
        for (int i = 0; i <= cols.length; i++) {
            ArrayList<Expr> exprList = new ArrayList<Expr>();
            for (int j = 0; j < i; j++) {
                Expr expr = new SlotRef(new TableName("testdb", "t"), cols[j]);
                exprList.add(expr);
            }
            groupingExprsList.add(exprList);
        }

        SelectStmt selectStmt = new SelectStmt(null, null, null, groupingExprsList, null, null, null);
        List<BitSet> bitSetList = selectStmt.getGroupingSetList();

        String[] result = {"{}", "{0}", "{0, 2}"};

        for(int i = 0; i < bitSetList.size(); i++) {
            String s = bitSetList.get(i).toString();
            //System.out.println(s);
            Assert.assertEquals(result[i], s);
        }
    }

    @Test
    public void testCube() {
        List<ArrayList<Expr>> groupingExprsList = new ArrayList<>();
        String[] cols = {"k1", "k2", "k3"};
        int size = cols.length == 0 ? 0 : 1 << (cols.length);
        for (int i = 0; i < size; i++) {
            ArrayList<Expr> exprList = new ArrayList<Expr>();
            int k = i;
            for (int j = 0; j < cols.length; j++) {
                if ((k & 1) == 1) {
                    Expr expr = new SlotRef(new TableName("testdb", "t"), cols[j]);
                    exprList.add(expr);
                }
                k >>= 1;
            }
            groupingExprsList.add(exprList);
        }

        SelectStmt selectStmt = new SelectStmt(null, null, null, groupingExprsList, null, null, null);
        List<BitSet> bitSetList = selectStmt.getGroupingSetList();

        String[] result = {"{}", "{1}", "{0}", "{0, 1}", "{2}", "{1, 2}", "{0, 2}"};

        for(int i = 0; i < bitSetList.size(); i++) {
            String s = bitSetList.get(i).toString();
            //System.out.println(s);
            Assert.assertEquals(result[i], s);
        }
    }
}
