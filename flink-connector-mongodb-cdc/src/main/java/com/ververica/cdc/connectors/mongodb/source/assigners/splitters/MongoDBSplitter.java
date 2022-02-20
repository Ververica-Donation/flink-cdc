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

package com.ververica.cdc.connectors.mongodb.source.assigners.splitters;

import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSnapshotSplit;

import java.util.Collection;

/**
 * The {@code ChunkSplitter} used to split collection into a set of chunks for MongoDB data source.
 */
public interface MongoDBSplitter {

    Collection<MongoDBSnapshotSplit> split(MongoDBSplitContext splitContext);
}
