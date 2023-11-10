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

import com.ververica.cdc.common.serializer.ListSerializer;
import com.ververica.cdc.common.types.DataField;
import com.ververica.cdc.common.types.RowType;

import java.io.IOException;
import java.util.Collections;

/** A {@link TypeSerializer} for {@link RowType}. */
public class RowTypeSerializer extends TypeSerializer<RowType> {
    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final RowTypeSerializer INSTANCE =
            new RowTypeSerializer(new ListSerializer<>(DataFieldSerializer.INSTANCE));

    private final ListSerializer<DataField> fieldsSerializer;

    public RowTypeSerializer(ListSerializer<DataField> fieldsSerializer) {
        this.fieldsSerializer = fieldsSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<RowType> duplicate() {
        return new RowTypeSerializer(new ListSerializer<>(DataFieldSerializer.INSTANCE));
    }

    @Override
    public RowType createInstance() {
        return new RowType(Collections.emptyList());
    }

    @Override
    public RowType copy(RowType from) {
        return from;
    }

    @Override
    public RowType copy(RowType from, RowType reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(RowType record, DataOutputView target) throws IOException {
        target.writeBoolean(record.isNullable());
        fieldsSerializer.serialize(record.getFields(), target);
    }

    @Override
    public RowType deserialize(DataInputView source) throws IOException {
        boolean nullable = source.readBoolean();
        return new RowType(nullable, fieldsSerializer.deserialize(source));
    }

    @Override
    public RowType deserialize(RowType reuse, DataInputView source) throws IOException {
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
    public TypeSerializerSnapshot<RowType> snapshotConfiguration() {
        return new RowTypeSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class RowTypeSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<RowType> {

        public RowTypeSerializerSnapshot() {
            super(() -> new RowTypeSerializer(new ListSerializer<>(DataFieldSerializer.INSTANCE)));
        }
    }
}
