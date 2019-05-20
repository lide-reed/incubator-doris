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
    : ExecNode(pool, tnode, descs),
    _repeat_id_list(tnode.repeat_node.repeat_id_list),
    _tuple_id(tnode.repeat_node.output_tuple_id),
    _new_slot_id(tnode.repeat_node.new_slot_id),
    _tuple_desc(nullptr),
    _child_row_batch(nullptr),
    _child_eos(false),
    _repeat_id_idx(-1),
    _runtime_state(nullptr) {
}

RepeatNode::~RepeatNode() {
}

Status RepeatNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == NULL) {
        return Status("Failed to get tuple descriptor.");
    }

    _new_slot_desc = state->desc_tbl().get_slot_descriptor(_new_slot_id);
    if (_new_slot_desc == NULL) {
        return Status("Failed to get new slot descriptor.");
    }
    DCHECK(_new_slot_desc->col_name() == "GROUPING__ID");

    return Status::OK;
}

Status RepeatNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));
    return Status::OK;
}

int RepeatNode::conver_to_int(int repeat_id_idx) {
    int result = 0;
    int size = _tuple_desc->slots().size();
    if (repeat_id_idx < 0) {
        for(int i = 0; i < size; i++) {
            result = result * 10 + 1;
        }
        return result;
    }

    std::vector<bool> repeat_ids = _repeat_id_list[repeat_id_idx];
    DCHECK_EQ(size, repeat_ids.size());
    for(int i = 0; i < repeat_ids.size(); i++) {
        result = result * 10;
        if (repeat_ids[i]) {
            result += 1;
        }
    }
    return result;
}

Status RepeatNode::get_repeated_batch(
            RowBatch* child_row_batch, int repeat_id_idx, RowBatch* row_batch) {

    DCHECK(repeat_id_idx >= -1);
    DCHECK(repeat_id_idx < _repeat_id_list.size());
    DCHECK(child_row_batch != nullptr);
    DCHECK_EQ(row_batch->num_rows(), 0);

    child_row_batch->deep_copy_to(row_batch);
    //++_num_rows_returned;
    //COUNTER_SET(_rows_returned_counter, _num_rows_returned);

    for (int i = 0; i < row_batch->num_rows(); i++) {
        TupleRow* row = row_batch->get_row(i);
        Tuple* tuple = row->get_tuple(0);
        if (tuple == NULL) {
            //TODO process
            continue;
        }
        for (int j = 0; j < _tuple_desc->slots().size(); j++) {
            const SlotDescriptor* slot_desc = _tuple_desc->slots()[i];
            if (slot_desc->id() == _new_slot_id) {
                DCHECK(slot_desc->col_name() == "GROUPING__ID");
                int* groupingId = reinterpret_cast<int*>(tuple->get_slot(slot_desc->tuple_offset()));
                if (groupingId != NULL) {
                    *groupingId = conver_to_int(repeat_id_idx);
                }
            }

            if (repeat_id_idx < 0) continue;

            std::vector<bool> repeat_ids = _repeat_id_list[repeat_id_idx];
            DCHECK(repeat_ids.size() == _tuple_desc->slots().size());
            if (!repeat_ids[j]) {
                tuple->set_null(slot_desc->null_indicator_offset());
            }
        }
    }

    return Status::OK;
}

Status RepeatNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);

    // current child has finished its repeat, get child's next batch
    if (_child_row_batch.get() == nullptr) {
        _child_eos = false;
        _child_row_batch.reset(
                    new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
        RETURN_IF_ERROR(child(0)->get_next(state, _child_row_batch.get(), &_child_eos));
        if (_child_eos || _child_row_batch == nullptr) {
            *eos = true;
            return Status::OK;
        }
    }

    DCHECK_EQ(row_batch->num_rows(), 0);
    RETURN_IF_ERROR(get_repeated_batch(_child_row_batch.get(), _repeat_id_idx, row_batch));
    _repeat_id_idx++;

    if (_repeat_id_idx >= _repeat_id_list.size()) {
        _child_row_batch.reset(nullptr);
        _repeat_id_idx = -1;
    }

    return Status::OK;
}

Status RepeatNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK;
    }
    RETURN_IF_ERROR(child(0)->close(state));
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

