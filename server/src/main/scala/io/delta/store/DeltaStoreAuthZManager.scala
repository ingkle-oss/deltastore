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

import io.delta.store.authorizations.DeltaStoreAuthZServerManager

case class DeltaStoreAuthZListFiles(
    filters: Seq[String] = Seq.empty[String]
)

class DeltaStoreAuthZManager(storeConfig: DeltaStoreConfig) {
  def checkListShares(token: String): Unit = {}

  def checkListSchemas(token: String, share: String): Unit = {}

  def checkListTables(token: String, share: String, schema: String): Unit = {}

  def checkListAllTables(token: String, share: String): Unit = {}

  def checkListFiles(
      token: String,
      share: String,
      schema: String,
      table: String
  ): DeltaStoreAuthZListFiles = {
    DeltaStoreAuthZListFiles()
  }
}

object DeltaStoreAuthZManager {
  private var instance: DeltaStoreAuthZManager = null

  def use(storeConfig: DeltaStoreConfig): Unit = {
    storeConfig.authorization.name.toUpperCase() match {
      case "SERVER" => instance = new DeltaStoreAuthZServerManager(storeConfig)
      case _        => instance = new DeltaStoreAuthZManager(storeConfig)
    }
  }

  def apply(): DeltaStoreAuthZManager = { instance }
}
