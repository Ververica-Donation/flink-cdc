/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDataSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.utils.ObjectUtils.doubleCompare;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.DECODING_PLUGIN_NAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.HEARTBEAT_INTERVAL;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.PG_PORT;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SLOT_NAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link Factory} to create {@link PostgresDataSource}. */
@Internal
public class PostgresDataSourceFactory implements DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresDataSourceFactory.class);

    public static final String IDENTIFIER = "postgres";

    @Override
    public DataSource createDataSource(Context context) {
        final Configuration config = context.getFactoryConfiguration();
        String hostname = config.get(HOSTNAME);
        int port = config.get(PG_PORT);
        String pluginName = config.get(DECODING_PLUGIN_NAME);
        String slotName = config.get(SLOT_NAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String chunkKeyColumn = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        String tables = config.get(TABLES);
        String tablesExclude = config.get(TABLES_EXCLUDE);
        Duration heartbeatInterval = config.get(HEARTBEAT_INTERVAL);
        StartupOptions startupOptions = getStartupOptions(config);

        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);

        double distributionFactorUpper = config.get(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower = config.get(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);

        boolean closeIdleReaders = config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);

        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(CONNECTION_POOL_SIZE);
        boolean skipSnapshotBackfill = config.get(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);

        validateIntegerOption(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
        validateIntegerOption(CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
        validateIntegerOption(SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
        validateIntegerOption(CONNECTION_POOL_SIZE, connectionPoolSize, 1);
        validateIntegerOption(CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        validateDistributionFactorUpper(distributionFactorUpper);
        validateDistributionFactorLower(distributionFactorLower);

        Map<String, String> configMap = config.toMap();
        String firstTable = tables.split(",")[0];
        TableId tableId = TableId.parse(firstTable);

        PostgresSourceConfigFactory configFactory =
                PostgresSourceBuilder.PostgresIncrementalSource.<RowData>builder()
                        .hostname(hostname)
                        .port(port)
                        .database(tableId.getNamespace())
                        .schemaList(".*")
                        .tableList(".*")
                        .username(username)
                        .password(password)
                        .decodingPluginName(pluginName)
                        .slotName(slotName)
                        .debeziumProperties(getDebeziumProperties(configMap))
                        .splitSize(splitSize)
                        .splitMetaGroupSize(splitMetaGroupSize)
                        .distributionFactorUpper(distributionFactorUpper)
                        .distributionFactorLower(distributionFactorLower)
                        .fetchSize(fetchSize)
                        .connectTimeout(connectTimeout)
                        .connectMaxRetries(connectMaxRetries)
                        .connectionPoolSize(connectionPoolSize)
                        .startupOptions(startupOptions)
                        .chunkKeyColumn(chunkKeyColumn)
                        .heartbeatInterval(heartbeatInterval)
                        .closeIdleReaders(closeIdleReaders)
                        .skipSnapshotBackfill(skipSnapshotBackfill)
                        .getConfigFactory();

        Selectors selectors = new Selectors.SelectorsBuilder().includeTables(tables).build();
        List<String> capturedTables = getTableList(configFactory.create(0), selectors);
        if (capturedTables.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot find any table by the option 'tables' = " + tables);
        }
        if (tablesExclude != null) {
            Selectors selectExclude =
                    new Selectors.SelectorsBuilder().includeTables(tablesExclude).build();
            List<String> excludeTables = getTableList(configFactory.create(0), selectExclude);
            if (!excludeTables.isEmpty()) {
                capturedTables.removeAll(excludeTables);
            }
            if (capturedTables.isEmpty()) {
                throw new IllegalArgumentException(
                        "Cannot find any table with by the option 'tables.exclude'  = "
                                + tablesExclude);
            }
        }
        configFactory.tableList(capturedTables.toArray(new String[0]));

        return new PostgresDataSource(configFactory);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(TABLES);
        options.add(SLOT_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PG_PORT);
        options.add(TABLES_EXCLUDE);
        options.add(DECODING_PLUGIN_NAME);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_MAX_RETRIES);
        options.add(CONNECTION_POOL_SIZE);
        options.add(HEARTBEAT_INTERVAL);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        return options;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static List<String> getTableList(
            PostgresSourceConfig sourceConfig, Selectors selectors) {
        return PostgresSchemaUtils.listTables(sourceConfig, null).stream()
                .filter(selectors::isMatch)
                .map(TableId::toString)
                .collect(Collectors.toList());
    }

    /** Checks the value of given integer option is valid. */
    private void validateIntegerOption(
            ConfigOption<Integer> option, int optionValue, int exclusiveMin) {
        checkState(
                optionValue > exclusiveMin,
                String.format(
                        "The value of option '%s' must larger than %d, but is %d",
                        option.key(), exclusiveMin, optionValue));
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";

    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";

    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s], but was: %s",
                                SourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                modeString));
        }
    }

    /** Checks the value of given evenly distribution factor upper bound is valid. */
    private void validateDistributionFactorUpper(double distributionFactorUpper) {
        checkState(
                doubleCompare(distributionFactorUpper, 1.0d) >= 0,
                String.format(
                        "The value of option '%s' must larger than or equals %s, but is %s",
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
                        1.0d,
                        distributionFactorUpper));
    }

    /** Checks the value of given evenly distribution factor lower bound is valid. */
    private void validateDistributionFactorLower(double distributionFactorLower) {
        checkState(
                doubleCompare(distributionFactorLower, 0.0d) >= 0
                        && doubleCompare(distributionFactorLower, 1.0d) <= 0,
                String.format(
                        "The value of option '%s' must between %s and %s inclusively, but is %s",
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }
}
