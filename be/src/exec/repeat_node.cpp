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
#include "runtime/raw_value.h"
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
            result += (1 << i);
        }
        return result;
    }

    std::vector<bool> repeat_ids = _repeat_id_list[repeat_id_idx];
    DCHECK_EQ(size, repeat_ids.size());
    for(int i = 0; i < repeat_ids.size(); i++) {
        result += repeat_ids[i] ? (1 << i) : 0;
    }
    return result;
}

Status RepeatNode::get_repeated_batch(
            RowBatch* child_row_batch, int repeat_id_idx, RowBatch* row_batch) {

    DCHECK(repeat_id_idx >= -1);
    DCHECK(repeat_id_idx < (int)_repeat_id_list.size());
    DCHECK(child_row_batch != nullptr);
    DCHECK_EQ(row_batch->num_rows(), 0);

    LOG(INFO) << "###################\n";
    LOG(INFO) << child_row_batch->to_string();
    LOG(INFO) << "-------------------\n";

    MemPool* tuple_pool = row_batch->tuple_data_pool();
    int tuple_buffer_size = row_batch->capacity() * _tuple_desc->byte_size();
    void* tuple_buffer = tuple_pool->allocate(tuple_buffer_size);
    if (tuple_buffer == nullptr) {
        return Status("Allocate memory for row batch failed.");
    }
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
    for (int i = 0; i < child_row_batch->num_rows(); ++i) {
        int row_idx = row_batch->add_row();
        TupleRow* row = row_batch->get_row(row_idx);
        row->set_tuple(0, tuple);
        memset(tuple, 0, _tuple_desc->num_null_bytes());

        // GROUPING__ID located in index 0
        const SlotDescriptor* slot_desc = _tuple_desc->slots()[0];
        if (slot_desc->id() == _new_slot_id) {
            DCHECK(slot_desc->col_name() == "GROUPING__ID");
            int* groupingId = reinterpret_cast<int*>(tuple->get_slot(slot_desc->tuple_offset()));
            if (groupingId != NULL) {
                *groupingId = conver_to_int(repeat_id_idx);
            }
        }

        const vector<TupleDescriptor*>& tuple_descs = child_row_batch->row_desc().tuple_descriptors();
        DCHECK(tuple_descs.size() > 0);
        TupleDescriptor *child_desc = tuple_descs[0];
        TupleRow* child_row = child_row_batch->get_row(i);
        Tuple* src_tuple = child_row->get_tuple(0);
        DCHECK_EQ(child_desc->slots().size(), _tuple_desc->slots().size() - 1);
        for (int j = 1; j < _tuple_desc->slots().size(); j++) {
            SlotDescriptor* src_slot_desc = child_desc->slots()[j - 1];
            SlotDescriptor* dst_slot_desc = _tuple_desc->slots()[j];
            bool src_slot_null = src_tuple->is_null(src_slot_desc->null_indicator_offset());
            void* src_slot = NULL;
            if (!src_slot_null) src_slot = src_tuple->get_slot(src_slot_desc->tuple_offset());
            RawValue::write(src_slot, tuple, dst_slot_desc, tuple_pool);
            if (repeat_id_idx >= 0) {
                std::vector<bool> repeat_ids = _repeat_id_list[repeat_id_idx];
                DCHECK(repeat_ids.size() == _tuple_desc->slots().size());
                if (!repeat_ids[j]) {
                    tuple->set_null(slot_desc->null_indicator_offset());
                }
            }
        }

        row_batch->commit_last_row();
        char* new_tuple = reinterpret_cast<char*>(tuple);
        new_tuple += _tuple_desc->byte_size();
        tuple = reinterpret_cast<Tuple*>(new_tuple);
    }

    LOG(INFO) << "################### row_batch\n";
    LOG(INFO) << row_batch->to_string();
    LOG(INFO) << "-------------------\n";
    //++_num_rows_returned;
    //COUNTER_SET(_rows_returned_counter, _num_rows_returned);

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
        if (_child_eos || _child_row_batch.get() == nullptr) {
            *eos = true;
            return Status::OK;
        }
    }

    DCHECK_EQ(row_batch->num_rows(), 0);
    RETURN_IF_ERROR(get_repeated_batch(_child_row_batch.get(), _repeat_id_idx, row_batch));
    _repeat_id_idx++;

    int size = _repeat_id_list.size();
    if (_repeat_id_idx >= size) {
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

