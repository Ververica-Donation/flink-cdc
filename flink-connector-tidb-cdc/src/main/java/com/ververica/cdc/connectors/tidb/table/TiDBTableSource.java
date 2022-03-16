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

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.tidb.TDBSourceOptions;
import com.ververica.cdc.connectors.tidb.TiDBSource;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiTableInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a TiDB binlog from a logical
 * description.
 */
public class TiDBTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema physicalSchema;
    private final String hostname;
    private final String database;
    private final String tableName;
    private final String username;
    private final String password;
    private final String pdAddresses;
    private final StartupOptions startupOptions;
    private final Map<String, String> options;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public TiDBTableSource(
            ResolvedSchema physicalSchema,
            String hostname,
            String database,
            String tableName,
            String username,
            String password,
            String pdAddresses,
            StartupOptions startupOptions,
            Map<String, String> options) {
        this.physicalSchema = physicalSchema;
        this.hostname = checkNotNull(hostname);
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.pdAddresses = checkNotNull(pdAddresses);
        this.startupOptions = startupOptions;
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.options = options;
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        final TiConfiguration tiConf = TDBSourceOptions.getTiConfiguration(pdAddresses, options);
        try (final TiSession session = TiSession.create(tiConf)) {
            final TiTableInfo tableInfo = session.getCatalog().getTable(database, tableName);

            TypeInformation<RowData> typeInfo = scanContext.createTypeInformation(producedDataType);
            TiKVMetadataConverter[] metadataConverters = getMetadataConverters();
            RowDataTiKVSnapshotEventDeserializationSchema snapshotEventDeserializationSchema =
                    new RowDataTiKVSnapshotEventDeserializationSchema(
                            typeInfo, tableInfo, metadataConverters);
            RowDataTiKVChangeEventDeserializationSchema changeEventDeserializationSchema =
                    new RowDataTiKVChangeEventDeserializationSchema(
                            typeInfo, tableInfo, metadataConverters);

            TiDBSource.Builder<RowData> builder =
                    TiDBSource.<RowData>builder()
                            .hostname(hostname)
                            .database(database)
                            .tableList(tableName)
                            .username(username)
                            .password(password)
                            .startupOptions(startupOptions)
                            .tiConf(tiConf)
                            .tiTableInfo(tableInfo)
                            .snapshotEventDeserializer(snapshotEventDeserializationSchema)
                            .changeEventDeserializer(changeEventDeserializationSchema);
            return SourceFunctionProvider.of(builder.build(), false);
        } catch (final Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public DynamicTableSource copy() {
        TiDBTableSource source =
                new TiDBTableSource(
                        physicalSchema,
                        hostname,
                        database,
                        tableName,
                        username,
                        password,
                        pdAddresses,
                        startupOptions,
                        options);
        source.producedDataType = producedDataType;
        source.metadataKeys = metadataKeys;
        return source;
    }

    private TiKVMetadataConverter[] getMetadataConverters() {
        if (metadataKeys.isEmpty()) {
            return new TiKVMetadataConverter[0];
        }

        return metadataKeys.stream()
                .map(
                        key ->
                                Stream.of(
                                                TiKVReadableMetadata.createTiKVReadableMetadata(
                                                        database, tableName))
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(TiKVReadableMetadata::getConverter)
                .toArray(TiKVMetadataConverter[]::new);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TiDBTableSource that = (TiDBTableSource) o;
        return Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(startupOptions, that.startupOptions)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                hostname,
                database,
                username,
                password,
                tableName,
                startupOptions,
                producedDataType,
                options);
    }

    @Override
    public String asSummaryString() {
        return "TiDB-CDC";
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(TiKVReadableMetadata.createTiKVReadableMetadata(database, tableName))
                .collect(
                        Collectors.toMap(
                                TiKVReadableMetadata::getKey, TiKVReadableMetadata::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }
}
