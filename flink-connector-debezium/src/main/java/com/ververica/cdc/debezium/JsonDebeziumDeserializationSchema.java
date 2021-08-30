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

package com.ververica.cdc.debezium;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;

/**
 * A debezium-json message format implementation of {@link DebeziumDeserializationSchema} which
 * converts the received {@link SourceRecord} into JsonString.
 */
public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    static final JsonConverter CONVERTER = new JsonConverter();

    public JsonDebeziumDeserializationSchema() {
        config(false);
    }

    public JsonDebeziumDeserializationSchema(boolean includeSchema) {
        config(includeSchema);
    }

    private void config(boolean includeSchema) {
        HashMap<String, Object> configs = new HashMap<>();
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
        CONVERTER.configure(configs);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        byte[] bytes =
                CONVERTER.fromConnectData(record.topic(), record.valueSchema(), record.value());
        out.collect(new String(bytes));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
