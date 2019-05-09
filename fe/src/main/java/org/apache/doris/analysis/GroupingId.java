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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TSlotRef;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class GroupingId extends Expr {
    private static final Logger LOG = LogManager.getLogger(GroupingId.class);
    private String col;

    // results of analysis slot
    private SlotDescriptor desc;

    // Only used write
    private GroupingId() {
        super();
    }

    public GroupingId(String col) {
        super();
        this.col = col;
        this.type = Type.BIGINT;
    }

    protected GroupingId(GroupingId other) {
        super(other);
        col = other.col;
        desc = other.desc;
    }

    public void setDesc(SlotDescriptor desc) {
        this.desc = desc;
        this.type = desc.getType();
        analysisDone();
    }

    @Override
    public Expr clone() {
        return new GroupingId(this);
    }

    public boolean isHllType() {
        return this.type == Type.HLL;
    }

    public SlotDescriptor getDesc() {
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkNotNull(desc);
        return desc;
    }

    public SlotId getSlotId() {
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkNotNull(desc);
        return desc.getId();
    }


    @Override
    public void vectorizedAnalyze(Analyzer analyzer) {
        computeOutputColumn(analyzer);
    }

    @Override
    public void computeOutputColumn(Analyzer analyzer) {
        outputColumn = desc.getSlotOffset();
        LOG.debug("SlotRef: " + debugString() + " outputColumn: " + outputColumn);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    public String debugString() {
        ToStringHelper helper = Objects.toStringHelper(this);
        helper.add("col", col);
        helper.add("type", type.toSql());
        return helper.toString();
    }

    @Override
    public String toSqlImpl() {
        if (col != null) {
            return col;
        }
        return "";
    }

    @Override
    public String toMySql() {
        if (col != null) {
            return col;
        }
        return "";
    }

    @Override
    public String toColumnLabel() {
        return col;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.SLOT_REF;
        msg.slot_ref = new TSlotRef(desc.getId().asInt(), desc.getParent().getId().asInt());
        msg.setOutput_column(outputColumn);
    }

    @Override
    public void markAgg() {
        desc.setIsAgg(true);
    }

    @Override
    public int hashCode() {
        if (desc != null) {
            return desc.getId().hashCode();
        }
        return Objects.hashCode(col.toLowerCase());
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        GroupingId other = (GroupingId) obj;
        // check slot ids first; if they're both set we only need to compare those
        // (regardless of how the ref was constructed)
        if (desc != null && other.desc != null) {
            return desc.getId().equals(other.desc.getId());
        }
        if ((col == null) != (other.col == null)) {
            return false;
        }
        if (col != null && !col.toLowerCase().equals(other.col.toLowerCase())) {
            return false;
        }
        return true;
    }

    @Override
    protected boolean isConstantImpl() { return false; }

    @Override
    public boolean isBoundByTupleIds(List<TupleId> tids) {
        Preconditions.checkState(desc != null);
        for (TupleId tid: tids) {
            if (tid.equals(desc.getParent().getId())) return true;
        }
        return false;
    }

    @Override
    public boolean isBound(SlotId slotId) {
        Preconditions.checkState(isAnalyzed);
        return desc.getId().equals(slotId);
    }

    @Override
    public void getIds(List<TupleId> tupleIds, List<SlotId> slotIds) {
        Preconditions.checkState(type != Type.INVALID);
        Preconditions.checkState(desc != null);
        if (slotIds != null) {
            slotIds.add(desc.getId());
        }
        if (tupleIds != null) {
            tupleIds.add(desc.getParent().getId());
        }
    }

    public String getColumnName() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    @Override
    public boolean supportSerializable() {
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(false);
        Text.writeString(out, col);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        col = Text.readString(in);
    }

    public static GroupingId read(DataInput in) throws IOException {
        GroupingId groupingId = new GroupingId();
        groupingId.readFields(in);
        return groupingId;
    }
}
