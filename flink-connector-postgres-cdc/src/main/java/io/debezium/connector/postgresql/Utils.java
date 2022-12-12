/*
 * Copyright 2022 Ververica Inc.
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

package io.debezium.connector.postgresql;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.postgres.source.offset.PostgresOffset;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;

/** A utility class for accessing various Debezium package-private methods. */
public final class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static Lsn lastKnownLsn(PostgresOffsetContext ctx) {
        return ctx.lsn();
    }

    public static PostgresOffset currentOffset(PostgresConnection jdbcConnection) {
        Long lsn;
        long txId;
        try {
            lsn = jdbcConnection.currentXLogLocation();
            txId = jdbcConnection.currentTransactionId();
            LOGGER.trace("Read xlogStart at '{}' from transaction '{}'", Lsn.valueOf(lsn), txId);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error getting current Lsn/txId " + e.getMessage(), e);
        }

        try {
            jdbcConnection.commit();
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "JDBC connection fails to commit: " + e.getMessage(), e);
        }

        return new PostgresOffset(lsn, txId, Instant.MIN);
    }

    public static PostgresSchema refreshSchema(
            PostgresSchema schema,
            PostgresConnection pgConnection,
            boolean printReplicaIdentityInfo)
            throws SQLException {
        return schema.refresh(pgConnection, printReplicaIdentityInfo);
    }
}
