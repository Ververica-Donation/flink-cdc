/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.postgres.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.connectors.base.source.assigner.SplitAssigner;
import com.ververica.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.postgres.source.PostgresDialect;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;

/**
 * The Postgres source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
@Internal
public class PostgresSourceEnumerator extends IncrementalSourceEnumerator {

    private final PostgresDialect postgresDialect;

    public PostgresSourceEnumerator(
            SplitEnumeratorContext<SourceSplitBase> context,
            PostgresSourceConfig sourceConfig,
            SplitAssigner splitAssigner,
            PostgresDialect postgresDialect,
            Boundedness boundedness) {
        super(context, sourceConfig, splitAssigner, boundedness);
        this.postgresDialect = postgresDialect;
    }

    @Override
    public void start() {
        createSlotForGlobalStreamSplit();
        super.start();
    }

    /**
     * Create slot for the unique global stream split.
     *
     * <p>Currently all startup modes need read the stream split. We need open the slot before
     * reading the globalStreamSplit to catch all data changes.
     */
    private void createSlotForGlobalStreamSplit() {
        try (PostgresConnection connection = postgresDialect.openJdbcConnection()) {
            SlotState slotInfo =
                    connection.getReplicationSlotState(
                            postgresDialect.getSlotName(), postgresDialect.getPluginName());
            // skip creating the replication slot when the slot exists.
            if (slotInfo != null) {
                return;
            }
            PostgresReplicationConnection replicationConnection =
                    postgresDialect.openPostgresReplicationConnection(connection);
            replicationConnection.createReplicationSlot();
            replicationConnection.close(false);

        } catch (Throwable t) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Fail to get or create slot for global stream split, the slot name is %s. Due to: ",
                            postgresDialect.getSlotName()),
                    t);
        }
    }
}
