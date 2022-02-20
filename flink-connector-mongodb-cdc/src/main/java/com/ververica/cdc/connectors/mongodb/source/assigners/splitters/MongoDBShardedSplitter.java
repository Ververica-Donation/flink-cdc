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

import com.mongodb.MongoQueryException;
import com.mongodb.client.MongoClient;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSnapshotSplit;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.DROPPED_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.KEY_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.MAX_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.MIN_FIELD;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.UNAUTHORIZED_ERROR;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.readChunks;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.readCollectionMetadata;

/**
 * The Sharded Splitter
 *
 * <p>Split collections by shard and chunk.
 */
public class MongoDBShardedSplitter implements MongoDBSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSplitVectorSplitter.class);

    public static final MongoDBShardedSplitter INSTANCE = new MongoDBShardedSplitter();

    private boolean hasPermission = true;

    private MongoDBShardedSplitter() {}

    @Override
    public Collection<MongoDBSnapshotSplit> split(MongoDBSplitContext splitContext) {
        CollectionId collectionId = splitContext.getCollectionId();
        MongoClient mongoClient = splitContext.getMongoClient();

        if (!hasPermission) {
            LOG.warn(
                    "Unauthorized to read config.collections or config.chunks, fallback to SampleSplitter");
            return MongoDBSampleBucketSplitter.INSTANCE.split(splitContext);
        }

        List<BsonDocument> chunks;
        BsonDocument collectionMetadata;
        try {
            collectionMetadata = readCollectionMetadata(mongoClient, collectionId);
            if (!isValidShardedCollection(collectionMetadata)) {
                LOG.warn(
                        "Collection {} does not appear to be sharded, fallback to SampleSplitter.",
                        collectionId);
                return MongoDBSampleBucketSplitter.INSTANCE.split(splitContext);
            }
            chunks = readChunks(mongoClient, collectionMetadata);
        } catch (MongoQueryException e) {
            if (e.getErrorCode() == UNAUTHORIZED_ERROR) {
                LOG.warn(
                        "Unauthorized to read config.collections or config.chunks: {}, fallback to SampleSplitter.",
                        e.getErrorMessage());
                hasPermission = false;
            } else {
                LOG.warn(
                        "Read config.chunks collection failed: {}, fallback to SampleSplitter",
                        e.getErrorMessage());
            }
            return MongoDBSampleBucketSplitter.INSTANCE.split(splitContext);
        }

        if (chunks.isEmpty()) {
            LOG.warn(
                    "Collection {} does not appear to be sharded, fallback to SampleSplitter.",
                    collectionId);
            return MongoDBSampleBucketSplitter.INSTANCE.split(splitContext);
        }

        List<MongoDBSnapshotSplit> snapshotSplits = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            BsonDocument chunk = chunks.get(i);
            BsonDocument min = chunk.getDocument(MIN_FIELD);
            BsonDocument max = chunk.getDocument(MAX_FIELD);

            snapshotSplits.add(
                    MongoDBSnapshotSplit.of(
                            collectionId, i, min, max, collectionMetadata.getDocument(KEY_FIELD)));
        }

        return snapshotSplits;
    }

    private boolean isValidShardedCollection(BsonDocument collectionMetadata) {
        return collectionMetadata != null
                && !collectionMetadata.getBoolean(DROPPED_FIELD, BsonBoolean.FALSE).getValue();
    }
}
