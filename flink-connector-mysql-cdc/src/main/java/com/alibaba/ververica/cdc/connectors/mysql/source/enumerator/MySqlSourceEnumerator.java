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

package com.alibaba.ververica.cdc.connectors.mysql.source.enumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.FlinkRuntimeException;

import com.alibaba.ververica.cdc.connectors.mysql.source.assigner.MySqlSnapshotSplitAssigner;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorAckEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorRequestReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.SourceReaderReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplitKind;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A MySQL CDC source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
public class MySqlSourceEnumerator implements SplitEnumerator<MySqlSplit, MySqlSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceEnumerator.class);
    private static final long CHECK_EVENT_INTERVAL = 30_000L;

    private final SplitEnumeratorContext<MySqlSplit> context;
    private final MySqlSnapshotSplitAssigner snapshotSplitAssigner;

    private final Map<Integer, List<MySqlSplit>> assignedSnapshotSplits;
    private final Map<Integer, List<MySqlSplit>> assignedBinlogSplits;
    private final Map<Integer, List<Tuple2<String, BinlogOffset>>> receiveFinishedSnapshotSplits;

    public MySqlSourceEnumerator(
            SplitEnumeratorContext<MySqlSplit> context,
            MySqlSnapshotSplitAssigner snapshotSplitAssigner,
            Map<Integer, List<MySqlSplit>> assignedSnapshotSplits,
            Map<Integer, List<MySqlSplit>> assignedBinlogSplits,
            Map<Integer, List<Tuple2<String, BinlogOffset>>> receiveFinishedSnapshotSplits) {
        this.context = context;
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.assignedSnapshotSplits = assignedSnapshotSplits;
        this.assignedBinlogSplits = assignedBinlogSplits;
        this.receiveFinishedSnapshotSplits = receiveFinishedSnapshotSplits;
    }

    @Override
    public void start() {
        this.snapshotSplitAssigner.open();
        this.context.callAsync(
                this::getRegisteredReader,
                this::syncWithReaders,
                CHECK_EVENT_INTERVAL,
                CHECK_EVENT_INTERVAL);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }
        Optional<MySqlSplit> split = snapshotSplitAssigner.getNext(requesterHostname);
        // assign snapshot split firstly
        if (split.isPresent()) {
            context.assignSplit(split.get(), subtaskId);
            // record assigned splits
            recordAssignedSnapshotSplit(split.get(), subtaskId);
            LOG.info("Assign snapshot split {} for subtask {}", split.get(), subtaskId);
        } else {
            // no more snapshot split, try assign binlog split
            if (couldAssignBinlogSplit()) {
                LOG.info("The snapshot phase read finished, will read binlog continue");
                assignBinlogSplit(subtaskId);
                LOG.info("Assign binlog split for subtask {}", subtaskId);
                return;
            }
            // no more snapshot split, skip
            else if (noMoreSplits(subtaskId)) {
                // do not send signalNoMoreSplits, because this will
                // lead to SourceReader being FINISHED which will lead
                // to checkpoint fail finally.
                // TODO send signalNoMoreSplits after FLIP-147 finished
                LOG.info("No available split for subtask {}", subtaskId);
                return;
            }
            // the binlog split may can not assign due to snapshot splits report is
            // incomplete, tell reader report finished snapshot splits
            notifyReaderReportFinishedSplitsIfNeed(new Integer[] {subtaskId});
        }
    }

    @Override
    public void addSplitsBack(List<MySqlSplit> splits, int subtaskId) {
        List<MySqlSplit> snapshotSplits = new ArrayList<>();
        List<MySqlSplit> binlogSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            if (split.getSplitKind() == MySqlSplitKind.SNAPSHOT) {
                snapshotSplits.add(split);
            } else {
                binlogSplits.add(split);
            }
        }
        if (!snapshotSplits.isEmpty()) {
            snapshotSplitAssigner.addSplits(snapshotSplits);
        }
        if (!binlogSplits.isEmpty()) {
            if (context.registeredReaders().size() > 0) {
                int taskId = context.registeredReaders().keySet().iterator().next();
                context.assignSplit(binlogSplits.get(0), taskId);
                recordAssignedBinlogSplit(binlogSplits.get(0), taskId);
            } else {
                LOG.error("Reassign binlog split error, no alive readers.");
            }
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // do nothing
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof SourceReaderReportEvent) {
            LOG.info(
                    "The enumerator receive snapshot finished report event {} from subtask {}.",
                    sourceEvent,
                    subtaskId);
            SourceReaderReportEvent reportEvent = (SourceReaderReportEvent) sourceEvent;
            final List<Tuple2<String, BinlogOffset>> ackSpitsForReader =
                    receiveFinishedSnapshotSplits.getOrDefault(subtaskId, new ArrayList<>());

            ackSpitsForReader.addAll(reportEvent.getFinishedSplits());
            receiveFinishedSnapshotSplits.put(subtaskId, ackSpitsForReader);
            notifyReaderReceivedFinishedSplits(new Integer[] {subtaskId});
        }
    }

    @Override
    public MySqlSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new MySqlSourceEnumState(
                snapshotSplitAssigner.remainingSplits(),
                snapshotSplitAssigner.getAlreadyProcessedTables(),
                assignedSnapshotSplits,
                assignedBinlogSplits,
                receiveFinishedSnapshotSplits);
    }

    @Override
    public void close() throws IOException {
        this.snapshotSplitAssigner.close();
    }

    private void syncWithReaders(Integer[] subtaskIds, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list obtain registered readers due to ", t);
        }
        // when the SourceEnumerator restore or the communication failed between
        // SourceEnumerator and SourceReader, it may missed some notification event.
        // tell all SourceReader(s) what SourceEnumerator has received and
        // request SourceReader(s) report their finished splits
        notifyReaderReceivedFinishedSplits(subtaskIds);
        notifyReaderReportFinishedSplitsIfNeed(subtaskIds);
    }

    private void notifyReaderReceivedFinishedSplits(Integer[] subtaskIds) {
        if (hasAssignedBinlogSplit()) {
            return;
        }
        for (int subtaskId : subtaskIds) {
            List<String> splits =
                    receiveFinishedSnapshotSplits.getOrDefault(subtaskId, new ArrayList<>())
                            .stream()
                            .map(r -> r.f0)
                            .collect(Collectors.toList());
            if (!splits.isEmpty()) {
                EnumeratorAckEvent ackEvent = new EnumeratorAckEvent(splits);
                context.sendEventToSourceReader(subtaskId, ackEvent);
            }
        }
    }

    private void notifyReaderReportFinishedSplitsIfNeed(Integer[] subtaskIds) {
        // call reader report finished snapshot
        if (hasAssignedBinlogSplit()) {
            return;
        }
        for (int subtaskId : subtaskIds) {
            final List<MySqlSplit> assignedSplit =
                    assignedSnapshotSplits.getOrDefault(subtaskId, new ArrayList<>());
            final List<Tuple2<String, BinlogOffset>> ackSpitsForReader =
                    receiveFinishedSnapshotSplits.getOrDefault(subtaskId, new ArrayList<>());
            int assignedSnapshotSplitSize =
                    (int)
                            assignedSplit.stream()
                                    .filter(
                                            sqlSplit ->
                                                    sqlSplit.getSplitKind()
                                                            == MySqlSplitKind.SNAPSHOT)
                                    .count();
            if (assignedSnapshotSplitSize > ackSpitsForReader.size()) {
                context.sendEventToSourceReader(subtaskId, new EnumeratorRequestReportEvent());
                LOG.info(
                        "The enumerator call subtask {} to report its finished splits.", subtaskId);
            }
        }
    }

    private Integer[] getRegisteredReader() {
        return this.context.registeredReaders().keySet().toArray(new Integer[0]);
    }

    private boolean noMoreSplits(int subtaskId) {
        // the task may never be assigned split
        final List<MySqlSplit> assignedSplit = assignedSnapshotSplits.get(subtaskId);
        return assignedSplit == null || hasAssignedBinlogSplit();
    }

    private boolean hasAssignedBinlogSplit() {
        return assignedBinlogSplits.size() > 0;
    }

    private boolean couldAssignBinlogSplit() {
        final long assignedSnapshotSplit =
                assignedSnapshotSplits.values().stream().mapToLong(Collection::size).sum();
        final long receiveSnapshotSplits =
                receiveFinishedSnapshotSplits.values().stream().mapToLong(Collection::size).sum();
        // All assigned snapshot splits have finished
        return assignedSnapshotSplit == receiveSnapshotSplits && assignedSnapshotSplit > 0;
    }

    private void assignBinlogSplit(int requestTaskId) {
        final List<MySqlSplit> assignedSnapshotSplit =
                assignedSnapshotSplits.values().stream()
                        .flatMap(Collection::stream)
                        .sorted(Comparator.comparing(MySqlSplit::splitId))
                        .collect(Collectors.toList());
        final List<Tuple2<String, BinlogOffset>> receiveSnapshotSplits =
                receiveFinishedSnapshotSplits.values().stream()
                        .flatMap(Collection::stream)
                        .sorted(Comparator.comparing(o -> o.f0))
                        .collect(Collectors.toList());

        final List<Tuple5<TableId, String, Object[], Object[], BinlogOffset>> snapshotSplits =
                new ArrayList<>();
        final Map<TableId, SchemaRecord> databaseHistory = new HashMap<>();

        BinlogOffset minBinlogOffset = receiveSnapshotSplits.get(0).f1;
        for (int i = 0; i < assignedSnapshotSplit.size(); i++) {
            MySqlSplit split = assignedSnapshotSplit.get(i);
            // find the min binlog offset
            if (receiveSnapshotSplits.get(i).f1.compareTo(minBinlogOffset) < 0) {
                minBinlogOffset = receiveSnapshotSplits.get(i).f1;
            }
            Tuple2<String, BinlogOffset> splitPosition = receiveSnapshotSplits.get(i);
            snapshotSplits.add(
                    Tuple5.of(
                            split.getTableId(),
                            split.getSplitId(),
                            split.getSplitBoundaryStart(),
                            split.getSplitBoundaryEnd(),
                            splitPosition.f1));
            databaseHistory.putAll(split.getDatabaseHistory());
        }

        final MySqlSplit lastSnapshotSplit =
                assignedSnapshotSplit.get(assignedSnapshotSplit.size() - 1);
        MySqlSplit binlogSplit =
                new MySqlSplit(
                        MySqlSplitKind.BINLOG,
                        lastSnapshotSplit.getTableId(),
                        "binlog-split-" + requestTaskId,
                        lastSnapshotSplit.getSplitBoundaryType(),
                        null,
                        null,
                        null,
                        null,
                        true,
                        minBinlogOffset,
                        snapshotSplits,
                        databaseHistory);
        // assign
        context.assignSplit(binlogSplit, requestTaskId);
        // record assigned splits
        recordAssignedBinlogSplit(binlogSplit, requestTaskId);
    }

    private void recordAssignedSnapshotSplit(MySqlSplit split, int subtaskId) {
        List<MySqlSplit> assignedSplits =
                this.assignedSnapshotSplits.getOrDefault(subtaskId, new ArrayList<>());
        assignedSplits.add(split);
        this.assignedSnapshotSplits.put(subtaskId, assignedSplits);
    }

    private void recordAssignedBinlogSplit(MySqlSplit split, int subtaskId) {
        List<MySqlSplit> assignedSplits =
                this.assignedBinlogSplits.getOrDefault(subtaskId, new ArrayList<>());
        assignedSplits.add(split);
        this.assignedBinlogSplits.put(subtaskId, assignedSplits);
    }
}
