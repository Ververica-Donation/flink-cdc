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

package org.apache.flink.cdc.common.source;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.types.DataType;

import java.io.Serializable;
import java.util.Map;

/** A metadata column that the source supports. */
@Experimental
public interface SupportedMetadataColumn extends Serializable {
    /** Column name. */
    String getName();

    /** The data type of this column in Flink CDC. */
    DataType getType();

    /** The returned java class of the reader. */
    Class<?> getJavaClass();

    /** Read the metadata from the dataChangeEvent. */
    Object read(Map<String, String> metadata);
}
