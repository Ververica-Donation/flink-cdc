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

package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.debezium.event.DebeziumSchemaDataTypeInference;

import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import org.apache.kafka.connect.data.Schema;

/** {@link DataType} inference for Oracle debezium {@link Schema}. */
@Internal
public class OracleSchemaDataTypeInference extends DebeziumSchemaDataTypeInference {

    private static final long serialVersionUID = 1L;

    protected DataType inferStruct(Object value, Schema schema) {
        // the Geometry datatype in Oracle will be converted to
        // a String with Json format
        if (Point.LOGICAL_NAME.equals(schema.name())
                || Geometry.LOGICAL_NAME.equals(schema.name())) {
            return DataTypes.STRING();
        } else {
            return super.inferStruct(value, schema);
        }
    }
}
