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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Wraps all information of group by clause.
 */
public class GroupByClause implements ParseNode {
    private final static Logger LOG = LogManager.getLogger(GroupByClause.class);

    public enum GroupingType {
        GROUP_BY,
        GROUPING_SETS,
        ROLLUP,
        CUBE;
    };

    private boolean analyzed_ = false;
    private GroupingType groupingType;
    private ArrayList<Expr> groupingExprs;
    private List<BitSet> groupingIDBitSetList;

    // reserve this info for toSQL
    private List<ArrayList<Expr>> groupingSetList;

    public GroupByClause(List<ArrayList<Expr>> groupingSetList, GroupingType type) {
        this.groupingType = type;
        this.groupingSetList = groupingSetList;
        Preconditions.checkState(type == GroupingType.GROUPING_SETS);
        buildGroupingClause(groupingSetList);
    }

    public GroupByClause(ArrayList<Expr> groupingExprs, GroupingType type) {
        this.groupingType = type;
        this.groupingExprs = groupingExprs;
        if (type == GroupingType.CUBE || type == GroupingType.ROLLUP) {
            buildGroupingClause(groupingExprs, type);
        }
        Preconditions.checkState(type != GroupingType.GROUPING_SETS);
    }

    public ArrayList<Expr> getGroupingExprs() {
        return groupingExprs;
    }

    public void setGroupingExprs(ArrayList<Expr> groupingExprs) {
        this.groupingExprs = groupingExprs;
    }

    protected GroupByClause(GroupByClause other) {
        this.groupingType = other.groupingType;
        this.groupingExprs = Expr.cloneAndResetList(groupingExprs);
        this.groupingIDBitSetList = other.groupingIDBitSetList;
        this.groupingSetList = other.groupingSetList;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (analyzed_) return;
        // disallow subqueries in the GROUP BY clause
        for (Expr expr: groupingExprs) {
            if (expr.contains(Predicates.instanceOf(Subquery.class))) {
                throw new AnalysisException(
                        "Subqueries are not supported in the GROUP BY clause.");
            }
        }

        boolean isSimpleGroupBy = isSimpleGroupBy();
        for (Expr groupingExpr : groupingExprs) {
            groupingExpr.analyze(analyzer);
            if (groupingExpr.contains(Expr.isAggregatePredicate())) {
                // reference the original expr in the error msg
                throw new AnalysisException(
                        "GROUP BY expression must not contain aggregate functions: "
                                + groupingExpr.toSql());
            }
            if (groupingExpr.contains(AnalyticExpr.class)) {
                // reference the original expr in the error msg
                throw new AnalysisException(
                        "GROUP BY expression must not contain analytic expressions: "
                                + groupingExpr.toSql());
            }

            if (groupingExpr.type.isHllType()) {
                throw new AnalysisException(
                        "GROUP BY expression must not contain hll column: "
                                + groupingExpr.toSql());
            }

            // only support column or column reference in composed GroupBy clause
            if(isSimpleGroupBy) {
                if (!(groupingExpr instanceof SlotRef)) {
                    throw new AnalysisException(groupingType.toString() +
                            "expression must be column or column reference: "
                                    + groupingExpr.toSql());
                }
            }
        }
        analyzed_ = true;
    }

    private boolean isSimpleGroupBy() {
        if (groupingType == GroupingType.GROUP_BY) {
            return true;
        }

        if (groupingType == GroupingType.GROUPING_SETS ||
                groupingSetList == null || groupingSetList.size() <= 1) {
            return true;
        }
    }

    @Override
    public String toSql() {
        StringBuilder strBuilder = new StringBuilder();
        switch (groupingType) {
            case GROUP_BY:
                if (groupingExprs != null) {
                    for (int i = 0; i < groupingExprs.size(); ++i) {
                        strBuilder.append(groupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
                    }
                }
                break;
            case GROUPING_SETS:
                if (groupingSetList != null) {
                    strBuilder.append(" GROUPING SETS (");
                    boolean first = true;
                    for(List<Expr> groupingExprs : groupingSetList) {
                        if (first) {
                            strBuilder.append("(");
                            first = false;
                        } else {
                            strBuilder.append(", (");
                        }
                        for (int i = 0; i < groupingExprs.size(); ++i) {
                            strBuilder.append(groupingExprs.get(i).toSql());
                            strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
                        }
                        strBuilder.append(")");
                    }
                    strBuilder.append(")");
                }
                break;
            case CUBE:
                if (groupingExprs != null) {
                    strBuilder.append("CUBE (");
                    for (int i = 0; i < groupingExprs.size(); ++i) {
                        strBuilder.append(groupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
                    }
                    strBuilder.append(")");
                }
                break;
            case ROLLUP:
                if (groupingExprs != null) {
                    strBuilder.append("ROLLUP (");
                    for (int i = 0; i < groupingExprs.size(); ++i) {
                        strBuilder.append(groupingExprs.get(i).toSql());
                        strBuilder.append((i + 1 != groupingExprs.size()) ? ", " : "");
                    }
                    strBuilder.append(")");
                }
                break;
            default:
                break;
        }
        return strBuilder.toString();
    }

    @Override
    public GroupByClause clone() {
        return new GroupByClause(this);
    }

    public void reset() {
        if (groupingExprs != null) Expr.resetList(groupingExprs);
        this.analyzed_ = false;
    }

    public boolean isEmpty() {
        return groupingExprs == null || groupingExprs.isEmpty();
    }

    // for CUBE or ROLLUP
    private void buildGroupingClause(ArrayList<Expr> groupingExprs, GroupingType groupingType) {
        if (groupingExprs == null || groupingExprs.isEmpty()) {
            return;
        }

        // remove repeated element
        Set<Expr> groupingExprSet = new HashSet<>();
        groupingExprSet.addAll(groupingExprs);
        groupingExprs.clear();
        groupingExprs.addAll(groupingExprSet);

        BitSet bitSetBase = new BitSet();
        bitSetBase.set(0, groupingExprs.size());

        groupingIDBitSetList = new ArrayList<>();
        if (groupingType == GroupingType.CUBE) {
            int size = (1 << groupingExprs.size()) - 1;
            for(int i = 0; i < size; i++) {
                String s = Integer.toBinaryString(i);
                BitSet bitSet = new BitSet();
                for(int j = 0; j < s.length(); j++) {
                    bitSet.set(s.length() - j - 1, s.charAt(j) == '1');
                }
                groupingIDBitSetList.add(bitSet);
            }
        } else if (groupingType == GroupingType.ROLLUP) {
            for(int i = 0; i < groupingExprs.size(); i++) {
                BitSet bitSet = new BitSet();
                bitSet.set(0, i);
                groupingIDBitSetList.add(bitSet);
            }
        }
    }

    // just for GROUPING SETS
    private void buildGroupingClause(List<ArrayList<Expr>> groupingSetList) {
        if (groupingSetList == null || groupingSetList.isEmpty()) {
            return;
        }

        // collect all Expr elements
        Set<Expr> groupingExprSet = new HashSet<>();
        for(ArrayList<Expr> list: groupingSetList) {
           groupingExprSet.addAll(list);
        }
        groupingExprs = new ArrayList<>(groupingExprSet);

        // regard as ordinary group by clause
        if (groupingSetList.size() <= 1) {
            return;
        }

        BitSet bitSetBase = new BitSet();
        bitSetBase.set(0, groupingExprs.size());

        groupingIDBitSetList = new ArrayList<>();
        for(ArrayList<Expr> list: groupingSetList) {
            BitSet bitSet = new BitSet();
            for(int i = 0; i < groupingExprs.size(); i++) {
                bitSet.set(i, list.contains(groupingExprs.get(i)));
            }
            if (!bitSet.equals(bitSetBase)) {
                if (!groupingIDBitSetList.contains(bitSet)) {
                    groupingIDBitSetList.add(bitSet);
                }
            }
        }
    }

    public List<BitSet> getGroupingIDBitSetList() {
        return groupingIDBitSetList;
    }
}

