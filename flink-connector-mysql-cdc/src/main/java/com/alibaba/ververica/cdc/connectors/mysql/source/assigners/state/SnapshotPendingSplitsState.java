/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.source.assigners.state;

import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import io.debezium.relational.TableId;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A {@link PendingSplitsState} for pending snapshot splits. */
public class SnapshotPendingSplitsState extends PendingSplitsState {

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    private final List<TableId> alreadyProcessedTables;

    /** The splits in the checkpoint. */
    private final List<MySqlSnapshotSplit> remainingSplits;

    /**
     * The snapshot splits that the {@link MySqlSourceEnumerator} has assigned to {@link
     * MySqlSplitReader}s.
     */
    private final Map<String, MySqlSnapshotSplit> assignedSplits;

    /**
     * The offsets of finished (snapshot) splits that the {@link MySqlSourceEnumerator} has received
     * from {@link MySqlSplitReader}s.
     */
    private final Map<String, BinlogOffset> splitFinishedOffsets;

    /**
     * Whether the snapshot split assigner is finished, which indicates there is no more splits and
     * all records of splits have been completely processed in the pipeline.
     */
    private final boolean isAssignerFinished;

    public SnapshotPendingSplitsState(
            List<TableId> alreadyProcessedTables,
            List<MySqlSnapshotSplit> remainingSplits,
            Map<String, MySqlSnapshotSplit> assignedSplits,
            Map<String, BinlogOffset> splitFinishedOffsets,
            boolean isAssignerFinished) {
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        this.assignedSplits = assignedSplits;
        this.splitFinishedOffsets = splitFinishedOffsets;
        this.isAssignerFinished = isAssignerFinished;
    }

    public List<TableId> getAlreadyProcessedTables() {
        return alreadyProcessedTables;
    }

    public List<MySqlSnapshotSplit> getRemainingSplits() {
        return remainingSplits;
    }

    public Map<String, MySqlSnapshotSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<String, BinlogOffset> getSplitFinishedOffsets() {
        return splitFinishedOffsets;
    }

    public boolean isAssignerFinished() {
        return isAssignerFinished;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnapshotPendingSplitsState that = (SnapshotPendingSplitsState) o;
        return isAssignerFinished == that.isAssignerFinished
                && Objects.equals(alreadyProcessedTables, that.alreadyProcessedTables)
                && Objects.equals(remainingSplits, that.remainingSplits)
                && Objects.equals(assignedSplits, that.assignedSplits)
                && Objects.equals(splitFinishedOffsets, that.splitFinishedOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                alreadyProcessedTables,
                remainingSplits,
                assignedSplits,
                splitFinishedOffsets,
                isAssignerFinished);
    }

    @Override
    public String toString() {
        return "SnapshotPendingSplitsState{"
                + "alreadyProcessedTables="
                + alreadyProcessedTables
                + ", remainingSplits="
                + remainingSplits
                + ", assignedSplits="
                + assignedSplits
                + ", splitFinishedOffsets="
                + splitFinishedOffsets
                + ", isAssignerFinished="
                + isAssignerFinished
                + '}';
    }
}
