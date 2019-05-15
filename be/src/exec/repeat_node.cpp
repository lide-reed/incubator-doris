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

#include "exec/repeat_node.h"

#include "exprs/expr.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris {

RepeatNode::RepeatNode(ObjectPool* pool, const TPlanNode& tnode,
                     const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs) {
    Status status = init(tnode, nullptr);
    DCHECK(status.ok()) << "RepeatNode c'tor: init failed: \n" << status.get_error_msg();
}

RepeatNode::~RepeatNode() {
}

Status RepeatNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Expr::create_expr_trees(
                    _pool, tnode.repeat_node.input_exprs, &_input_expr_ctxs));
    _repeat_id_list = tnode.repeat_node.repeat_id_list;
    return Status::OK;
}

Status RepeatNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    return Status::OK;
}

Status RepeatNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));
    child(0)->close(state);
    return Status::OK;
}

Status RepeatNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);

    DCHECK_EQ(row_batch->num_rows(), 0);

    *eos = true;
    return Status::OK;
}

Status RepeatNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK;
    }
    return ExecNode::close(state);
}

void RepeatNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "RepeatNode(";
    //TODO output content of RepeatNode
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

}

