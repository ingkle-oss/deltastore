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

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.google.common.hash.Hashing
import io.delta.sharing.server.protocol.{Schema, Share, Table}
import org.slf4j.LoggerFactory

import io.delta.store.configurations._

class DeltaStoreConfigManager(storeConfig: DeltaStoreConfig) {
  private val logger = LoggerFactory.getLogger(classOf[DeltaStoreConfigManager])

  protected def getFilesystemConfigLoader()
      : CacheLoader[String, DeltaFilesystemConfig] = {
    new CacheLoader[String, DeltaFilesystemConfig]() {
      def load(key: String): DeltaFilesystemConfig = {
        new DeltaFilesystemConfig()
      }
    }
  }

  protected def getShareConfigLoader()
      : CacheLoader[String, DeltaShareConfig] = {
    new CacheLoader[String, DeltaShareConfig]() {
      def load(key: String): DeltaShareConfig = {
        new DeltaShareConfig()
      }
    }
  }

  protected def getSchemaConfigLoader()
      : CacheLoader[String, DeltaSchemaConfig] = {
    new CacheLoader[String, DeltaSchemaConfig]() {
      def load(key: String): DeltaSchemaConfig = {
        new DeltaSchemaConfig()
      }
    }
  }

  protected def getTableConfigLoader()
      : CacheLoader[String, DeltaTableConfig] = {
    new CacheLoader[String, DeltaTableConfig]() {
      def load(key: String): DeltaTableConfig = {
        new DeltaTableConfig()
      }
    }
  }

  private lazy val filesystemConfigCache = {
    val builder = CacheBuilder.newBuilder()
    if (storeConfig.configCacheDuration > 0) {
      builder.expireAfterAccess(
        storeConfig.configCacheDuration,
        TimeUnit.MINUTES
      )
    }
    builder.maximumSize(storeConfig.configCacheSize)
    builder.build[String, DeltaFilesystemConfig](getFilesystemConfigLoader())
  }

  private lazy val shareConfigCache = {
    val builder = CacheBuilder.newBuilder()
    if (storeConfig.configCacheDuration > 0) {
      builder.expireAfterAccess(
        storeConfig.configCacheDuration,
        TimeUnit.MINUTES
      )
    }
    builder.maximumSize(storeConfig.configCacheSize)
    builder.build[String, DeltaShareConfig](getShareConfigLoader())
  }

  private lazy val schemaConfigCache = {
    val builder = CacheBuilder.newBuilder()
    if (storeConfig.configCacheDuration > 0) {
      builder.expireAfterAccess(
        storeConfig.configCacheDuration,
        TimeUnit.MINUTES
      )
    }
    builder.maximumSize(storeConfig.configCacheSize)
    builder.build[String, DeltaSchemaConfig](getSchemaConfigLoader())
  }

  private lazy val tableConfigCache = {
    val builder = CacheBuilder.newBuilder()
    if (storeConfig.configCacheDuration > 0) {
      builder.expireAfterAccess(
        storeConfig.configCacheDuration,
        TimeUnit.MINUTES
      )
    }
    builder.maximumSize(storeConfig.configCacheSize)
    builder.build[String, DeltaTableConfig](getTableConfigLoader())
  }

  def getFilesystem(filesystemName: String): DeltaFilesystemConfig = {
    try {
      val key = filesystemName
      val config = filesystemConfigCache.get(key)
      config
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        null
    }
  }

  def getShare(shareName: String): DeltaShareConfig = {
    try {
      val key = shareName
      val config = shareConfigCache.get(key)
      config
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        null
    }
  }

  def getSchema(shareName: String, schemaName: String): DeltaSchemaConfig = {
    try {
      val key = List(shareName, schemaName).mkString(".")
      val config = schemaConfigCache.get(key)
      config
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        null
    }
  }

  def getTable(
      shareName: String,
      schemaName: String,
      tableName: String
  ): DeltaTableConfig = {
    try {
      val key = List(shareName, schemaName, tableName).mkString(".")
      val config = tableConfigCache.get(key)
      config
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        null
    }
  }

  def listFilesystems(
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaFilesystemConfig], Option[String]) = {
    Nil -> None
  }

  def listShares(
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaShareConfig], Option[String]) = {
    Nil -> None
  }

  def listSchemas(
      shareName: String,
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaSchemaConfig], Option[String]) = {
    Nil -> None
  }

  def listTables(
      shareName: String,
      schemaName: String,
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaTableConfig], Option[String]) = {
    Nil -> None
  }

  def listAllTables(
      shareName: String,
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaTableConfig], Option[String]) = {
    Nil -> None
  }
}

object DeltaStoreConfigManager {
  private var instance: DeltaStoreConfigManager = null

  def use(storeConfig: DeltaStoreConfig): Unit = {
    storeConfig.configuration.name.toUpperCase() match {
      case "SERVER" => instance = new DeltaStoreConfigServerManager(storeConfig)
      case _        => instance = new DeltaStoreConfigFileManager(storeConfig)
    }
  }

  def apply(): DeltaStoreConfigManager = { instance }
}
