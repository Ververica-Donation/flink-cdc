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

package org.apache.flink.cdc.runtime.operators.schema;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.RouteBehavior;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistryProvider;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;

import java.time.Duration;
import java.util.List;

/** Factory to create {@link SchemaOperator}. */
@Internal
public class SchemaOperatorFactory extends SimpleOperatorFactory<Event>
        implements CoordinatedOperatorFactory<Event>, OneInputStreamOperatorFactory<Event, Event> {

    private static final long serialVersionUID = 1L;

    private final MetadataApplier metadataApplier;
    private final List<Tuple2<String, TableId>> routingRules;
    private final RouteBehavior routeBehavior;

    public SchemaOperatorFactory(
            MetadataApplier metadataApplier,
            List<Tuple2<String, TableId>> routingRules,
            Duration rpcTimeOut,
            RouteBehavior routeBehavior) {
        super(new SchemaOperator(routingRules, rpcTimeOut, routeBehavior));
        this.metadataApplier = metadataApplier;
        this.routingRules = routingRules;
        this.routeBehavior = routeBehavior;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new SchemaRegistryProvider(
                operatorID, operatorName, metadataApplier, routingRules, routeBehavior);
    }
}
