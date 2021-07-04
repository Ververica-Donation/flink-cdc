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

package com.alibaba.ververica.cdc.connectors.mysql.source.utils;

import org.apache.flink.table.types.logical.RowType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utils to prepare SQL statement. */
public class StatementUtils {

    private StatementUtils() {}

    public static String buildSplitBoundaryQuery(
            TableId tableId,
            RowType pkRowType,
            boolean isFirstSplit,
            boolean isLastSplit,
            int maxSplitSize) {
        return buildSplitQuery(tableId, pkRowType, isFirstSplit, isLastSplit, maxSplitSize, true);
    }

    public static String buildSplitScanQuery(
            TableId tableId, RowType pkRowType, boolean isFirstSplit, boolean isLastSplit) {
        return buildSplitQuery(tableId, pkRowType, isFirstSplit, isLastSplit, -1, false);
    }

    private static String buildSplitQuery(
            TableId tableId,
            RowType pkRowType,
            boolean isFirstSplit,
            boolean isLastSplit,
            int limitSize,
            boolean onlyScanBoundary) {
        String condition = null;

        if (isFirstSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ?");
            sql.append(" AND NOT (");
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ?");
            sql.append(")");
            condition = sql.toString();
        } else if (isLastSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ?");
            condition = sql.toString();
        } else {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ?");
            if (!onlyScanBoundary) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ?");
                sql.append(")");
            }
            sql.append(" AND ");
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ?");
            condition = sql.toString();
        }

        final String orderBy = pkRowType.getFieldNames().stream().collect(Collectors.joining(", "));
        if (onlyScanBoundary) {
            return buildSelectWithBoundaryRowLimits(
                    tableId,
                    limitSize,
                    getPrimaryKeyColumnsProjection(pkRowType),
                    getMaxPrimaryKeyColumnsProjection(pkRowType),
                    Optional.ofNullable(condition),
                    orderBy);
        } else {
            return buildSelectWithRowLimits(
                    tableId, limitSize, "*", Optional.ofNullable(condition), orderBy);
        }
    }

    public static PreparedStatement readTableSplitStatement(
            JdbcConnection jdbc,
            String sql,
            boolean isFirstSplit,
            boolean isLastSplit,
            Object[] maxPrimaryKey,
            Object[] prevSplitEnd,
            int primaryKeyNum,
            int fetchSize) {
        try {
            final PreparedStatement statement = initStatement(jdbc, sql, fetchSize);
            if (isFirstSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, maxPrimaryKey[i]);
                    statement.setObject(i + 1 + primaryKeyNum, maxPrimaryKey[i]);
                }
            } else if (isLastSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, maxPrimaryKey[i]);
                }
            } else {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, prevSplitEnd[i]);
                    statement.setObject(i + 1 + primaryKeyNum, maxPrimaryKey[i]);
                }
            }
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build the table split statement.", e);
        }
    }

    public static PreparedStatement readTableSplitDataStatement(
            JdbcConnection jdbc,
            String sql,
            boolean isFirstSplit,
            boolean isLastSplit,
            Object[] splitStart,
            Object[] splitEnd,
            int primaryKeyNum,
            int fetchSize) {
        try {
            final PreparedStatement statement = initStatement(jdbc, sql, fetchSize);
            if (isFirstSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitEnd[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                }
            } else if (isLastSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                }
            } else {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                    statement.setObject(i + 1 + 2 * primaryKeyNum, splitEnd[i]);
                }
            }
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build the split data read statement.", e);
        }
    }

    public static String buildMaxPrimaryKeyQuery(TableId tableId, RowType pkRowType) {
        final String orderBy =
                pkRowType.getFieldNames().stream().collect(Collectors.joining(" DESC, ")) + " DESC";
        return buildSelectWithRowLimits(
                tableId,
                1,
                pkRowType.getFieldNames().stream().collect(Collectors.joining(",")),
                Optional.empty(),
                orderBy);
    }

    public static String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    public static String quote(TableId tableId) {
        return quote(tableId.catalog()) + "." + quote(tableId.table());
    }

    private static PreparedStatement initStatement(JdbcConnection jdbc, String sql, int fetchSize)
            throws SQLException {
        final Connection connection = jdbc.connection();
        connection.setAutoCommit(false);
        final PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }

    private static void addPrimaryKeyColumnsToCondition(
            RowType pkRowType, StringBuilder sql, String predicate) {
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next()).append(predicate);
            if (fieldNamesIt.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    private static String getPrimaryKeyColumnsProjection(RowType pkRowType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append(fieldNamesIt.next());
            if (fieldNamesIt.hasNext()) {
                sql.append(" , ");
            }
        }
        return sql.toString();
    }

    private static String getMaxPrimaryKeyColumnsProjection(RowType pkRowType) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
                fieldNamesIt.hasNext(); ) {
            sql.append("MAX(" + fieldNamesIt.next() + ")");
            if (fieldNamesIt.hasNext()) {
                sql.append(" , ");
            }
        }
        return sql.toString();
    }

    private static String buildSelectWithRowLimits(
            TableId tableId,
            int limit,
            String projection,
            Optional<String> condition,
            String orderBy) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(projection).append(" FROM ");
        sql.append(quotedTableIdString(tableId));
        if (condition.isPresent()) {
            sql.append(" WHERE ").append(condition.get());
        }
        sql.append(" ORDER BY ").append(orderBy);
        if (limit > 0) {
            sql.append(" LIMIT ").append(limit);
        }
        return sql.toString();
    }

    private static String buildSelectWithBoundaryRowLimits(
            TableId tableId,
            int limit,
            String projection,
            String maxColumnProjection,
            Optional<String> condition,
            String orderBy) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(maxColumnProjection);
        sql.append(" FROM (");
        sql.append("SELECT ");
        sql.append(projection);
        sql.append(" FROM ");
        sql.append(quotedTableIdString(tableId));
        if (condition.isPresent()) {
            sql.append(" WHERE ").append(condition.get());
        }
        sql.append(" ORDER BY ").append(orderBy).append(" LIMIT ").append(limit);
        sql.append(") T");
        return sql.toString();
    }

    private static String quotedTableIdString(TableId tableId) {
        return tableId.toQuotedString('`');
    }
}
