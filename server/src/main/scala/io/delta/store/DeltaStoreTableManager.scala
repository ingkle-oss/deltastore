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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.slf4j.LoggerFactory

import io.delta.store.helpers._
import io.delta.store.tables._

class DeltaStoreTableManager(storeConfig: DeltaStoreConfig) {
  def getTable(tableConfig: DeltaTableConfig): DeltaStoreTable = {
    new DeltaStoreTable(tableConfig, storeConfig)
  }

  def register(): Unit = {}
  def update(): Unit = {}

  def ready(): Unit = {}
}

class DeltaStoreTableCacheManager(storeConfig: DeltaStoreConfig)
    extends DeltaStoreTableManager(storeConfig: DeltaStoreConfig) {
  private val logger =
    LoggerFactory.getLogger(classOf[DeltaStoreTableCacheManager])

  protected val configManager = DeltaStoreConfigManager()

  private lazy val cacheSyncDuration = {
    storeConfig.repository.configs.find(c =>
      c.key == "cacheSyncDuration"
    ) match {
      case Some(c) => c.value.toInt
      case None    => 3000
    }
  }

  private lazy val enableCacheSync = {
    storeConfig.repository.configs.find(c => c.key == "enableCacheSync") match {
      case Some(c) => c.value.toBoolean
      case None    => true
    }
  }

  private lazy val enableAutoRegister = {
    storeConfig.repository.configs.find(c =>
      c.key == "enableAutoRegister"
    ) match {
      case Some(c) => c.value.toBoolean
      case None    => false
    }
  }

  private val tableSets =
    scala.collection.mutable.Map[String, DeltaStoreTable]()

  override def getTable(tableConfig: DeltaTableConfig): DeltaStoreTable = {
    val table = tableSets.getOrElseUpdate(
      tableConfig.id,
      storeConfig.repository.name.toUpperCase() match {
        case "DB" =>
          logger.info(
            s"Registered ${tableConfig.share}/${tableConfig.schema}/${tableConfig.name} table"
          )
          new DeltaStoreDBTable(tableConfig, storeConfig)
      }
    )
    table
  }

  override def register(): Unit = {
    logger.info(s"Register all tables")

    var shares: (Seq[DeltaShareConfig], Option[String]) = (Nil, None)
    do {
      shares = configManager.listShares(shares._2)
      shares._1.foreach(s => {
        var tables: (Seq[DeltaTableConfig], Option[String]) =
          (Nil, None)
        do {
          tables = configManager.listAllTables(s.name, tables._2)
          tables._1.foreach(t => {
            getTable(t)
          })
        } while (tables._2.isDefined)
      })
    } while (shares._2.isDefined)
  }

  override def update(): Unit = {
    logger.info(s"Synchronize all tables")

    for ((id, table) <- tableSets) {
      logger.info(s"Update ${id} table")

      try {
        table.update()
      } catch {
        case e: Exception => logger.error(e.getMessage, e)
      }
    }
  }

  override def ready(): Unit = {
    if (enableCacheSync || enableAutoRegister) {
      val thread = new Thread {
        override def run() {
          while (true) {
            try {
              if (enableAutoRegister) { register() }
              if (enableCacheSync) { update() }
            } finally {
              Thread.sleep(cacheSyncDuration)
            }
          }
        }
      }
      thread.start()
    }
  }
}

object DeltaStoreTableManager {
  private var instance: DeltaStoreTableManager = null

  def use(storeConfig: DeltaStoreConfig): Unit = {
    storeConfig.repository.name.toUpperCase() match {
      case _ => instance = new DeltaStoreTableCacheManager(storeConfig)
    }
  }

  def apply(): DeltaStoreTableManager = { instance }
}
