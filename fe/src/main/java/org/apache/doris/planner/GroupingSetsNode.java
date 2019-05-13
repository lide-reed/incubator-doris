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

package org.apache.doris.planner;

import com.google.common.base.Preconditions;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.GroupByClause;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TGroupingSetsNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Used for grouping sets.
 * It will add some new rows and a column of groupingId according to grouping sets info.
 */
public class GroupingSetsNode extends PlanNode {

    private List<Expr> groupingExprs;
    private List<BitSet> groupingIdList;

    public GroupingSetsNode(PlanNodeId id, ArrayList<TupleId> tupleIds) {
        super(id, tupleIds, "GROUPINGSETS");
        Preconditions.checkArgument(tupleIds.size() > 0);
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        avgRowSize = 0;
        cardinality = 0;
        numNodes = 1;
    }

    @Override
    public void init(Analyzer analyzer) {
        Preconditions.checkState(conjuncts.isEmpty());
        for (TupleId id: tupleIds) analyzer.getTupleDesc(id).setIsMaterialized(true);
        computeMemLayout(analyzer);
        computeStats(analyzer);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.GROUPING_SETS_NODE;
        List<Long> groupingIds = GroupByClause.convertGroupingId(groupingIdList);
        List<TExpr> groupingExprList = Expr.treesToThrift(groupingExprs);
        msg.grouping_sets_node = new TGroupingSetsNode(groupingExprList, groupingIds);
    }

}
