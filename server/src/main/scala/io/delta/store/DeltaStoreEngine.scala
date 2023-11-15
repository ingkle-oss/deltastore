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

trait DeltaStoreEngineBatch {
  def putProtocol(version: Option[Long] = None)(value: DeltaStoreProtocol): Unit
  def putMetadata(version: Option[Long] = None)(value: DeltaStoreMetadata): Unit
  def putAction(
      version: Option[Long] = None
  )(key: String, value: DeltaStoreAddFile): Unit
}

trait DeltaStoreEngine {
  def close(): Unit

  def flush(): Unit

  def getBatch(): DeltaStoreEngineBatch
  def putBatch(batch: DeltaStoreEngineBatch): Unit

  def hasLatestCommit(): Boolean
  def getLatestCommit(defvalue: Long = 0): Long
  def putLatestCommit(value: Long): Unit

  def hasProtocol(version: Option[Long] = None)(): Boolean
  def getProtocol(version: Option[Long] = None)(
      defvalue: DeltaStoreProtocol = null
  ): DeltaStoreProtocol
  def putProtocol(version: Option[Long] = None)(value: DeltaStoreProtocol): Unit

  def hasMetadata(version: Option[Long] = None)(): Boolean
  def getMetadata(
      version: Option[Long] = None
  )(defvalue: DeltaStoreMetadata = null): DeltaStoreMetadata
  def putMetadata(version: Option[Long] = None)(value: DeltaStoreMetadata): Unit

  def getAction(
      version: Option[Long] = None
  )(key: String, defvalue: DeltaStoreAddFile = null): DeltaStoreAddFile
  def putAction(
      version: Option[Long] = None
  )(key: String, value: DeltaStoreAddFile): Unit
  def deleteAction(version: Option[Long] = None)(key: String): Unit

  def getActions(version: Option[Long] = None)(
      prefix: String
  ): Seq[DeltaStoreAddFile]
  def getActionsRange(
      version: Option[Long] = None
  )(from: String, to: String): Seq[DeltaStoreAddFile]
  def getActionsAll(version: Option[Long] = None)(): Seq[DeltaStoreAddFile]
}
