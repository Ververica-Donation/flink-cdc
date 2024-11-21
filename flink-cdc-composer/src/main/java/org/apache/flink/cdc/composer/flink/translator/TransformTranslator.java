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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.composer.definition.ModelDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.definition.UdfDef;
import org.apache.flink.cdc.runtime.operators.transform.PostTransformOperator;
import org.apache.flink.cdc.runtime.operators.transform.PreTransformOperator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Translator used to build {@link PreTransformOperator} and {@link PostTransformOperator} for event
 * transform.
 */
public class TransformTranslator {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public DataStream<Event> translatePreTransform(
            DataStream<Event> input,
            List<TransformDef> transforms,
            List<UdfDef> udfFunctions,
            List<ModelDef> models) {
        if (transforms.isEmpty()) {
            return input;
        }

        PreTransformOperator.Builder preTransformFunctionBuilder =
                PreTransformOperator.newBuilder();
        for (TransformDef transform : transforms) {
            preTransformFunctionBuilder.addTransform(
                    transform.getSourceTable(),
                    transform.getProjection().orElse(null),
                    transform.getFilter().orElse(null),
                    transform.getPrimaryKeys(),
                    transform.getPartitionKeys(),
                    transform.getTableOptions());
        }

        preTransformFunctionBuilder.addUdfFunctions(
                udfFunctions.stream()
                        .map(this::udfDefToNameAndClasspathTuple)
                        .collect(Collectors.toList()));
        preTransformFunctionBuilder.addUdfFunctions(
                models.stream()
                        .map(this::modelToNameAndClasspathTuple)
                        .collect(Collectors.toList()));
        return input.transform(
                "Transform:Schema", new EventTypeInfo(), preTransformFunctionBuilder.build());
    }

    public DataStream<Event> translatePostTransform(
            DataStream<Event> input,
            List<TransformDef> transforms,
            String timezone,
            List<UdfDef> udfFunctions,
            List<ModelDef> models) {
        if (transforms.isEmpty()) {
            return input;
        }

        PostTransformOperator.Builder postTransformFunctionBuilder =
                PostTransformOperator.newBuilder();
        for (TransformDef transform : transforms) {
            if (transform.isValidProjection() || transform.isValidFilter()) {
                postTransformFunctionBuilder.addTransform(
                        transform.getSourceTable(),
                        transform.isValidProjection() ? transform.getProjection().get() : null,
                        transform.isValidFilter() ? transform.getFilter().get() : null,
                        transform.getPrimaryKeys(),
                        transform.getPartitionKeys(),
                        transform.getTableOptions());
            }
        }
        postTransformFunctionBuilder.addTimezone(timezone);
        postTransformFunctionBuilder.addUdfFunctions(
                udfFunctions.stream()
                        .map(this::udfDefToNameAndClasspathTuple)
                        .collect(Collectors.toList()));
        postTransformFunctionBuilder.addUdfFunctions(
                models.stream()
                        .map(this::modelToNameAndClasspathTuple)
                        .collect(Collectors.toList()));
        return input.transform(
                "Transform:Data", new EventTypeInfo(), postTransformFunctionBuilder.build());
    }

    private Tuple2<String, String> modelToNameAndClasspathTuple(ModelDef model) {
        try {
            // A tricky way to pass parameters to UDF
            String serializedParams = objectMapper.writeValueAsString(model.getParameters());
            return Tuple2.of(serializedParams, model.getModel());
        } catch (Exception e) {
            throw new IllegalArgumentException("ModelDef is illegal, ModelDef is " + model, e);
        }
    }

    private Tuple2<String, String> udfDefToNameAndClasspathTuple(UdfDef udf) {
        return Tuple2.of(udf.getName(), udf.getClasspath());
    }
}
