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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.doris.analysis.*;
import org.apache.doris.thrift.*;

import java.util.BitSet;
import java.util.List;

/**
 * Used for grouping sets.
 * It will add some new rows and a column of groupingId according to grouping sets info.
 */
public class RepeatNode extends PlanNode {

    private List<Expr> baseExprs;
    private List<BitSet> repeatIdList;

    public RepeatNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo, List<BitSet> repeatIdList) {
        super(id, aggInfo.getOutputTupleId().asList(), "REPEATNODE");
        this.children.add(input);
        this.baseExprs = aggInfo.getGroupingExprs();
        this.repeatIdList = repeatIdList;
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
        msg.node_type = TPlanNodeType.REPEAT_NODE;
        List<Long> repeatIds = GroupByClause.convertGroupingId(repeatIdList);
        List<TExpr> baseExprList = Expr.treesToThrift(baseExprs);
        msg.repeat_node = new TRepeatNode(baseExprList, repeatIds);
    }

    @Override
    protected String debugString() {
        return Objects.toStringHelper(this).add("Repeat", baseExprs).addValue(
                super.debugString()).toString();
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(detailPrefix + "repeat: \n(will add number of line:");
        output.append(repeatIdList.size());
        output.append(", columns: 1)\n");
        return output.toString();
    }
}
