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

package com.ververica.cdc.connectors.sqlserver.experimental.offset;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * A structure describes a fine grained offset in a binlog event including binlog position and gtid
 * set etc.
 *
 * <p>This structure can also be used to deal the binlog event in transaction, a transaction may
 * contains multiple change events, and each change event may contain multiple rows. When restart
 * from a specific {@link BinlogOffset}, we need to skip the processed change events and the
 * processed rows.
 */
public class BinlogOffset extends Offset {

    private static final long serialVersionUID = 1L;

    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String EVENTS_TO_SKIP_OFFSET_KEY = "event";
    public static final String ROWS_TO_SKIP_OFFSET_KEY = "row";
    public static final String GTID_SET_KEY = "gtids";
    public static final String TIMESTAMP_KEY = "ts_sec";
    public static final String SERVER_ID_KEY = "server_id";

    public static final BinlogOffset INITIAL_OFFSET = new BinlogOffset("", 0);
    public static final BinlogOffset NO_STOPPING_OFFSET = new BinlogOffset("", Long.MIN_VALUE);

    public BinlogOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public BinlogOffset(String filename, long position) {
        this(filename, position, 0L, 0L, 0L, null, null);
    }

    public BinlogOffset(
            String filename,
            long position,
            long restartSkipEvents,
            long restartSkipRows,
            long binlogEpochSecs,
            @Nullable String restartGtidSet,
            @Nullable Integer serverId) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(BINLOG_FILENAME_OFFSET_KEY, filename);
        offsetMap.put(BINLOG_POSITION_OFFSET_KEY, String.valueOf(position));
        offsetMap.put(EVENTS_TO_SKIP_OFFSET_KEY, String.valueOf(restartSkipEvents));
        offsetMap.put(ROWS_TO_SKIP_OFFSET_KEY, String.valueOf(restartSkipRows));
        offsetMap.put(TIMESTAMP_KEY, String.valueOf(binlogEpochSecs));
        if (restartGtidSet != null) {
            offsetMap.put(GTID_SET_KEY, restartGtidSet);
        }
        if (serverId != null) {
            offsetMap.put(SERVER_ID_KEY, String.valueOf(serverId));
        }
        this.offset = offsetMap;
    }

    public String getFilename() {
        return offset.get(BINLOG_FILENAME_OFFSET_KEY);
    }

    public long getPosition() {
        return longOffsetValue(offset, BINLOG_POSITION_OFFSET_KEY);
    }

    public long getRestartSkipEvents() {
        return longOffsetValue(offset, EVENTS_TO_SKIP_OFFSET_KEY);
    }

    public long getRestartSkipRows() {
        return longOffsetValue(offset, ROWS_TO_SKIP_OFFSET_KEY);
    }

    public String getGtidSet() {
        return offset.get(GTID_SET_KEY);
    }

    public long getTimestamp() {
        return longOffsetValue(offset, TIMESTAMP_KEY);
    }

    public Long getServerId() {
        return longOffsetValue(offset, SERVER_ID_KEY);
    }

    /**
     * This method is inspired by {@link io.debezium.relational.history.HistoryRecordComparator}.
     */
    @Override
    public int compareTo(Offset offset) {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BinlogOffset)) {
            return false;
        }
        BinlogOffset that = (BinlogOffset) o;
        return offset.equals(that.offset);
    }
}
