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

package com.ververica.cdc.common.serializer.schema;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.cdc.common.serializer.StringSerializer;
import com.ververica.cdc.common.types.DataField;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link DataField}. */
public class DataFieldSerializer extends TypeSerializer<DataField> {
    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final DataFieldSerializer INSTANCE =
            new DataFieldSerializer(StringSerializer.INSTANCE, DataTypeSerializer.INSTANCE);

    private final StringSerializer stringSerializer;
    private final DataTypeSerializer dataTypeSerializer;

    public DataFieldSerializer(
            StringSerializer stringSerializer, DataTypeSerializer dataTypeSerializer) {
        this.stringSerializer = stringSerializer;
        this.dataTypeSerializer = dataTypeSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<DataField> duplicate() {
        return new DataFieldSerializer(StringSerializer.INSTANCE, DataTypeSerializer.INSTANCE);
    }

    @Override
    public DataField createInstance() {
        return new DataField("unknown", DataTypes.BIGINT());
    }

    @Override
    public DataField copy(DataField from) {
        return from;
    }

    @Override
    public DataField copy(DataField from, DataField reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(DataField record, DataOutputView target) throws IOException {
        stringSerializer.serialize(record.getName(), target);
        dataTypeSerializer.serialize(record.getType(), target);
        stringSerializer.serialize(record.getDescription(), target);
    }

    @Override
    public DataField deserialize(DataInputView source) throws IOException {
        String name = stringSerializer.deserialize(source);
        DataType type = dataTypeSerializer.deserialize(source);
        String desc = stringSerializer.deserialize(source);
        return new DataField(name, type, desc);
    }

    @Override
    public DataField deserialize(DataField reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || (obj != null && obj.getClass() == getClass());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public TypeSerializerSnapshot<DataField> snapshotConfiguration() {
        return new DataFieldSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class DataFieldSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<DataField> {

        public DataFieldSerializerSnapshot() {
            super(
                    () ->
                            new DataFieldSerializer(
                                    StringSerializer.INSTANCE, DataTypeSerializer.INSTANCE));
        }
    }
}
