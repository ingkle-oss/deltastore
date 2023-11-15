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

package io.delta.store.configurations

import scala.collection.JavaConverters._

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.slf4j.LoggerFactory

import io.delta.store.{
  DeltaFilesystemConfig,
  DeltaSchemaConfig,
  DeltaShareConfig,
  DeltaStoreConfig,
  DeltaStoreConfigManager,
  DeltaTableConfig
}

class DeltaStoreConfigFileManager(storeConfig: DeltaStoreConfig)
    extends DeltaStoreConfigManager(storeConfig: DeltaStoreConfig) {
  private val logger =
    LoggerFactory.getLogger(classOf[DeltaStoreConfigFileManager])

  private val filesystems = storeConfig.filesystems
  private val shares = {
    storeConfig.shares.map(share => {
      val schemas = share.schemas.map(schema => {
        val tables = schema.tables.map(table =>
          table.copy(share = share.name, schema = schema.name)
        )
        schema.copy(share = share.name, tables = tables)
      })
      share.copy(schemas = schemas)
    })
  }

  protected override def getFilesystemConfigLoader()
      : CacheLoader[String, DeltaFilesystemConfig] = {
    new CacheLoader[String, DeltaFilesystemConfig]() {
      def load(key: String): DeltaFilesystemConfig = {
        try {
          filesystems
            .find(filesystem => filesystem.name == key)
            .getOrElse(new DeltaFilesystemConfig())
        } catch {
          case e: Exception => new DeltaFilesystemConfig()
        }
      }
    }
  }

  protected override def getShareConfigLoader()
      : CacheLoader[String, DeltaShareConfig] = {
    new CacheLoader[String, DeltaShareConfig]() {
      def load(key: String): DeltaShareConfig = {
        try {
          shares
            .find(share => share.name == key)
            .getOrElse(new DeltaShareConfig())
        } catch {
          case e: Exception => new DeltaShareConfig()
        }
      }
    }
  }

  protected override def getSchemaConfigLoader()
      : CacheLoader[String, DeltaSchemaConfig] = {
    new CacheLoader[String, DeltaSchemaConfig]() {
      def load(key: String): DeltaSchemaConfig = {
        try {
          val items = key.split('.')
          val shareName = items(0)
          val schemaName = items(1)
          val shareConfig = shares
            .find(share => share.name == shareName)
            .getOrElse(new DeltaShareConfig())
          val schemaConfig = shareConfig.schemas
            .find(schema => schema.name == schemaName)
            .getOrElse(new DeltaSchemaConfig())
          schemaConfig
        } catch {
          case e: Exception => new DeltaSchemaConfig()
        }
      }
    }
  }

  protected override def getTableConfigLoader()
      : CacheLoader[String, DeltaTableConfig] = {
    new CacheLoader[String, DeltaTableConfig]() {
      def load(key: String): DeltaTableConfig = {
        try {
          val items = key.split('.')
          val shareName = items(0)
          val schemaName = items(1)
          val tableName = items(2)
          val shareConfig = shares
            .find(share => share.name == shareName)
            .getOrElse(new DeltaShareConfig())
          val schemaConfig = shareConfig.schemas
            .find(schema => schema.name == schemaName)
            .getOrElse(new DeltaSchemaConfig())
          val tableConfig = schemaConfig.tables
            .find(table => table.name == tableName)
            .getOrElse(new DeltaTableConfig())
          tableConfig
        } catch {
          case e: Exception => new DeltaTableConfig()
        }
      }
    }
  }

  override def listFilesystems(
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaFilesystemConfig], Option[String]) = {
    val list = filesystems

    list -> None
  }

  override def listShares(
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaShareConfig], Option[String]) = {
    val list = shares

    list -> None
  }

  override def listSchemas(
      shareName: String,
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaSchemaConfig], Option[String]) = {
    val shareConfig = shares
      .find(share => share.name == shareName)
      .getOrElse(new DeltaShareConfig())
    val list = shareConfig.schemas

    list -> None
  }

  override def listTables(
      shareName: String,
      schemaName: String,
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaTableConfig], Option[String]) = {
    val shareConfig = shares
      .find(share => share.name == shareName)
      .getOrElse(new DeltaShareConfig())
    val schemaConfig = shareConfig.schemas
      .find(schema => schema.name == schemaName)
      .getOrElse(new DeltaSchemaConfig())
    val list = schemaConfig.tables

    list -> None
  }

  override def listAllTables(
      shareName: String,
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaTableConfig], Option[String]) = {
    val shareConfig = shares
      .find(share => share.name == shareName)
      .getOrElse(new DeltaShareConfig())
    val list = shareConfig.schemas.flatMap(schema => schema.tables)

    list -> None
  }
}
