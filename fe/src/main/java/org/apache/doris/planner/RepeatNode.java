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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Used for grouping sets.
 * It will add some new rows and a column of groupingId according to grouping sets info.
 */
public class RepeatNode extends PlanNode {

    private List<Expr> baseExprs;
    private List<BitSet> repeatIdList;
    private TupleDescriptor tupleDescriptor;

    public RepeatNode(PlanNodeId id, PlanNode input, List<BitSet> repeatIdList, TupleDescriptor tupleDesc) {
        super(id, input.getTupleIds(), "REPEATNODE");
        this.children.add(input);
        this.repeatIdList = regenerateRepeatIdList(repeatIdList);
        this.tupleDescriptor = tupleDesc;
        tupleIds.add(tupleDescriptor.getId());
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        avgRowSize = 0;
        cardinality = 0;
        numNodes = 1;
    }

    private static TupleDescriptor createTupleDescriptor(TupleDescriptor inputDesc) {
        TupleDescriptor tupleDescriptor = null;
        for(SlotDescriptor slotDescriptor : inputDesc.getSlots()) {

        }

        return tupleDescriptor;
    }

    //
    private List<BitSet> regenerateRepeatIdList(List<BitSet> oldRepeatIdList) {
        List<BitSet> newRepeatIdList = null;
        return newRepeatIdList;
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
        List<Long> repeatIds = convertToLongList(repeatIdList);
        //TODO pass tupleid
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
        output.append(detailPrefix + "repeat: new add ");
        output.append(repeatIdList.size());
        output.append(" lines and 1 column\n");
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }

    public static List<Long> convertToLongList(List<BitSet> bitSetList) {
        List<Long> groupingIdList = new ArrayList<>();
        for(BitSet bitSet: bitSetList) {
            long l = 0L;
            for (int i = 0; i < bitSet.length(); ++i) {
                l += bitSet.get(i) ? (1L << i) : 0L;
            }
            groupingIdList.add(l);
        }
        return groupingIdList;
    }
}
