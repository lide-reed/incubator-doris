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
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TSlotRef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;

public class VirtualSlotRef extends SlotRef {
    private static final Logger LOG = LogManager.getLogger(VirtualSlotRef.class);
    private TableName tblName;
    private String col;
    // Used in toSql
    private String label;

    // results of analysis slot
    private SlotDescriptor desc;

    public VirtualSlotRef(String col, Type type) {
        super(null, col);
        this.type = type;
    }

    // C'tor for a "pre-analyzed" ref to slot that doesn't correspond to
    // a table's column.
    public VirtualSlotRef(SlotDescriptor desc) {
        super(desc);
    }

    protected VirtualSlotRef(VirtualSlotRef other) {
        super(other);
    }

    protected VirtualSlotRef(SlotRef other) {
        super(other);
    }

    @Override
    public Expr clone() {
        return new VirtualSlotRef(this);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        desc = analyzer.registerVirtualColumnRef(col, type);
        type = desc.getType();
        if (this.type == Type.CHAR) {
            this.type = Type.VARCHAR;
        }
        if (!type.isSupported()) {
            throw new AnalysisException(
                    "Unsupported type '" + type.toString() + "' in '" + toSql() + "'.");
        }
        numDistinctValues = desc.getStats().getNumDistinctValues();
        if (type == Type.BOOLEAN) {
            selectivity = DEFAULT_SELECTIVITY;
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.SLOT_REF;
        msg.slot_ref = new TSlotRef(desc.getId().asInt(), -1);
        msg.setOutput_column(outputColumn);
    }

    public static VirtualSlotRef read(DataInput in) throws IOException {
        SlotRef slotRef = SlotRef.read(in);
        return new VirtualSlotRef(slotRef);
    }
}
