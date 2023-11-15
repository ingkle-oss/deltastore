/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.store

case class DeltaStoreAddFile(
    path: String,
    partitionValues: Map[String, String] = Map.empty[String, String],
    size: Long = 0,
    stats: String = null
)

case class DeltaStoreFormat(provider: String = "parquet")

case class DeltaStoreMetadata(
    id: String = null,
    name: String = null,
    description: String = null,
    format: DeltaStoreFormat = DeltaStoreFormat(),
    schemaString: String = null,
    configuration: Map[String, String] = Map.empty,
    partitionColumns: Seq[String] = Nil
)

case class DeltaStoreProtocol(minReaderVersion: Int)
