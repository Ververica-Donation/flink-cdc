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

package com.ververica.cdc.connectors.mysql.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlParallelSource;
import com.ververica.cdc.connectors.mysql.source.MySqlParallelSourceBuilder;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a MySQL binlog source from a logical
 * description.
 */
public class MySqlTableSource implements ScanTableSource {

    private final TableSchema physicalSchema;
    private final int port;
    private final String hostname;
    private final String database;
    private final String username;
    private final String password;
    private final String serverId;
    private final String tableName;
    private final ZoneId serverTimeZone;
    private final Properties dbzProperties;
    private final boolean enableParallelRead;
    private final int splitSize;
    private final int fetchSize;
    private final Duration connectTimeout;
    private final StartupOptions startupOptions;

    public MySqlTableSource(
            TableSchema physicalSchema,
            int port,
            String hostname,
            String database,
            String tableName,
            String username,
            String password,
            ZoneId serverTimeZone,
            Properties dbzProperties,
            @Nullable String serverId,
            boolean enableParallelRead,
            int splitSize,
            int fetchSize,
            Duration connectTimeout,
            StartupOptions startupOptions) {
        this.physicalSchema = physicalSchema;
        this.port = port;
        this.hostname = checkNotNull(hostname);
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.serverId = serverId;
        this.serverTimeZone = serverTimeZone;
        this.dbzProperties = dbzProperties;
        this.enableParallelRead = enableParallelRead;
        this.splitSize = splitSize;
        this.fetchSize = fetchSize;
        this.connectTimeout = connectTimeout;
        this.startupOptions = startupOptions;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo =
                scanContext.createTypeInformation(physicalSchema.toRowDataType());
        DebeziumDeserializationSchema<RowData> deserializer =
                new RowDataDebeziumDeserializeSchema(
                        rowType, typeInfo, ((rowData, rowKind) -> {}), serverTimeZone);
        if (enableParallelRead) {
            MySqlParallelSource<RowData> parallelSource =
                    MySqlParallelSourceBuilder.<RowData>builder()
                            .hostname(hostname)
                            .port(port)
                            .databaseList(database)
                            .tableList(database + "." + tableName)
                            .username(username)
                            .password(password)
                            .serverTimeZone(serverTimeZone.toString())
                            .startupOptions(startupOptions)
                            .splitSize(splitSize)
                            .fetchSize(fetchSize)
                            .connectTimeout(connectTimeout)
                            .serverId(serverId)
                            .debeziumProperties(dbzProperties)
                            .deserializer(deserializer)
                            .build();
            return SourceProvider.of(parallelSource);
        } else {
            MySqlSource.Builder<RowData> builder =
                    MySqlSource.<RowData>builder()
                            .hostname(hostname)
                            .port(port)
                            .databaseList(database)
                            .tableList(database + "." + tableName)
                            .username(username)
                            .password(password)
                            .serverTimeZone(serverTimeZone.toString())
                            .debeziumProperties(dbzProperties)
                            .startupOptions(startupOptions)
                            .deserializer(deserializer);
            Optional.ofNullable(serverId)
                    .ifPresent(
                            serverId -> builder.serverId(MySqlSourceOptions.getServerId(serverId)));
            DebeziumSourceFunction<RowData> sourceFunction = builder.build();
            return SourceFunctionProvider.of(sourceFunction, false);
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new MySqlTableSource(
                physicalSchema,
                port,
                hostname,
                database,
                tableName,
                username,
                password,
                serverTimeZone,
                dbzProperties,
                serverId,
                enableParallelRead,
                splitSize,
                fetchSize,
                connectTimeout,
                startupOptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlTableSource)) {
            return false;
        }
        MySqlTableSource that = (MySqlTableSource) o;
        return port == that.port
                && enableParallelRead == that.enableParallelRead
                && splitSize == that.splitSize
                && fetchSize == that.fetchSize
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(serverTimeZone, that.serverTimeZone)
                && Objects.equals(dbzProperties, that.dbzProperties)
                && Objects.equals(connectTimeout, that.connectTimeout)
                && Objects.equals(startupOptions, that.startupOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                port,
                hostname,
                database,
                username,
                password,
                serverId,
                tableName,
                serverTimeZone,
                dbzProperties,
                enableParallelRead,
                splitSize,
                fetchSize,
                connectTimeout,
                startupOptions);
    }

    @Override
    public String asSummaryString() {
        return "MySQL-CDC";
    }
}
