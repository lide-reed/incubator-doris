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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/** replace SlotReference ExprId in logical plans */
public class ExprIdRewriter extends ExpressionRewrite {
    private final List<Rule> rules;
    private final JobContext jobContext;

    public ExprIdRewriter(ReplaceRule replaceRule, JobContext jobContext) {
        super(new ExpressionRuleExecutor(ImmutableList.of(bottomUp(replaceRule))));
        rules = buildRules();
        this.jobContext = jobContext;
    }

    /**rewriteExpr*/
    public Plan rewriteExpr(Plan plan, Map<ExprId, ExprId> replaceMap) {
        if (replaceMap.isEmpty()) {
            return plan;
        }
        for (Rule rule : rules) {
            Pattern<Plan> pattern = (Pattern<Plan>) rule.getPattern();
            if (pattern.matchPlanTree(plan)) {
                List<Plan> newPlans = rule.transform(plan, jobContext.getCascadesContext());
                Plan newPlan = newPlans.get(0);
                if (!newPlan.deepEquals(plan)) {
                    return newPlan;
                }
            }
        }
        return plan;
    }

    /**
     * Iteratively rewrites IDs using the replaceMap:
     * 1. For a given SlotReference with initial ID, retrieve the corresponding value ID from the replaceMap.
     * 2. If the value ID exists within the replaceMap, continue the lookup process using the value ID
     * until it no longer appears in the replaceMap.
     * 3. return SlotReference final value ID as the result of the rewrite.
     * e.g. replaceMap:{0:3, 1:6, 6:7}
     * SlotReference:a#0 -> a#3, a#1 -> a#7
     * */
    public static class ReplaceRule implements ExpressionPatternRuleFactory {
        private final Map<ExprId, ExprId> replaceMap;

        public ReplaceRule(Map<ExprId, ExprId> replaceMap) {
            this.replaceMap = replaceMap;
        }

        @Override
        public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
            return ImmutableList.of(
                    matchesType(SlotReference.class).thenApply(ctx -> {
                        Slot slot = ctx.expr;

                        ExprId newId = replaceMap.get(slot.getExprId());
                        if (newId == null) {
                            return slot;
                        }
                        ExprId lastId = newId;
                        while (true) {
                            newId = replaceMap.get(lastId);
                            if (newId == null) {
                                return slot.withExprId(lastId);
                            } else {
                                lastId = newId;
                            }
                        }
                    }).toRule(ExpressionRuleType.EXPR_ID_REWRITE_REPLACE)
            );
        }
    }
}
