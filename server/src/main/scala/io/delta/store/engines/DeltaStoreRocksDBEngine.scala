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

package io.delta.store.engines

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

import org.apache.spark.sql.types.StructType
import org.rocksdb._
import org.rocksdb.util.SizeUnit
import org.slf4j.LoggerFactory

import io.delta.store.{
  DeltaEngineConfig,
  DeltaSimpleConfig,
  DeltaStoreAddFile,
  DeltaStoreEngine,
  DeltaStoreEngineBatch,
  DeltaStoreMetadata,
  DeltaStoreProtocol
}
import io.delta.store.helpers._

class DeltaStoreRocksDBEngine(
    engineConfig: DeltaEngineConfig,
    name: String,
    readOnly: Boolean = false
) extends DeltaStoreEngine {
  private val logger =
    LoggerFactory.getLogger(classOf[DeltaStoreRocksDBEngine])

  private val metadataPrefixKey = "__metadata__"
  private val latestCommitKey = metadataPrefixKey + "latest_commit__"
  private val protocolKey = metadataPrefixKey + "protocol__"
  private val metadataKey = metadataPrefixKey + "metadata__"

  private val path = {
    engineConfig.configs.find(c => c.key == "path") match {
      case Some(c) => c.value
      case None    => "/tmp"
    }
  }
  private val compression = {
    engineConfig.configs.find(c => c.key == "compression") match {
      case Some(c) => c.value
      case None    => ""
    }
  }

  private val dbHandle: RocksDB = {
    val options = new Options()
      .setCreateIfMissing(true)
      .setWriteBufferSize(8 * SizeUnit.KB)
      .setMaxWriteBufferNumber(3)
      .setMaxBackgroundCompactions(5)
      .setCompactionStyle(CompactionStyle.UNIVERSAL)
    val tableConfig = new BlockBasedTableConfig()
    tableConfig.setFilter(new BloomFilter(10, false))
    options.setTableFormatConfig(tableConfig)
    if (compression.toUpperCase() == "LZ4") {
      options.setCompressionType(CompressionType.LZ4_COMPRESSION)
    } else if (compression.toUpperCase() == "SNAPPY") {
      options.setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    }

    var handle: RocksDB = null

    if (readOnly) {
      handle = RocksDB.openReadOnly(options, path + "/deltastore-" + name)
    } else {
      handle = RocksDB.open(options, path + "/deltastore-" + name)
    }

    options.close()

    handle
  }

  def close(): Unit = {
    dbHandle.close()
  }

  def flush(): Unit = {
    val options = new FlushOptions()

    dbHandle.flush(options)

    options.close()
  }

  class DeltaStoreRocksDBEngineBatch extends DeltaStoreEngineBatch {
    private val batchHandle = new WriteBatch()

    def handle(): WriteBatch = {
      batchHandle
    }

    def close(): Unit = {
      batchHandle.close()
    }

    def putProtocol(
        version: Option[Long] = None
    )(value: DeltaStoreProtocol): Unit = {
      batchHandle.put(
        protocolKey.getBytes(),
        JsonHelper.toJson[DeltaStoreProtocol](value).getBytes()
      )
    }

    def putMetadata(
        version: Option[Long] = None
    )(value: DeltaStoreMetadata): Unit = {
      batchHandle.put(
        metadataKey.getBytes(),
        JsonHelper.toJson[DeltaStoreMetadata](value).getBytes()
      )
    }

    def putAction(
        version: Option[Long] = None
    )(key: String, value: DeltaStoreAddFile): Unit = {
      batchHandle.put(
        key.getBytes(),
        JsonHelper.toJson[DeltaStoreAddFile](value).getBytes()
      )
    }
  }

  def getBatch(): DeltaStoreEngineBatch = {
    new DeltaStoreRocksDBEngineBatch()
  }

  def putBatch(batch: DeltaStoreEngineBatch): Unit = {
    val rsbatch = batch.asInstanceOf[DeltaStoreRocksDBEngineBatch]
    val options = new WriteOptions()

    dbHandle.write(options, rsbatch.handle())

    options.close()
    rsbatch.close()
  }

  def hasLatestCommit(): Boolean = {
    val value = dbHandle.get(latestCommitKey.getBytes())
    value != null
  }

  def getLatestCommit(defvalue: Long = 0): Long = {
    val value = dbHandle.get(latestCommitKey.getBytes())
    if (value != null) { new String(value).toLong }
    else { defvalue }
  }

  def putLatestCommit(value: Long): Unit = {
    dbHandle.put(latestCommitKey.getBytes(), value.toString.getBytes())
  }

  def hasProtocol(version: Option[Long] = None)(): Boolean = {
    val value = dbHandle.get(protocolKey.getBytes())
    value != null
  }

  def getProtocol(
      version: Option[Long] = None
  )(defvalue: DeltaStoreProtocol = null): DeltaStoreProtocol = {
    val value = dbHandle.get(protocolKey.getBytes())
    if (value != null) {
      JsonHelper.fromJson[DeltaStoreProtocol](new String(value))
    } else { defvalue }
  }

  def putProtocol(
      version: Option[Long] = None
  )(value: DeltaStoreProtocol): Unit = {
    dbHandle.put(
      protocolKey.getBytes(),
      JsonHelper.toJson[DeltaStoreProtocol](value).getBytes()
    )
  }

  def hasMetadata(version: Option[Long] = None)(): Boolean = {
    val value = dbHandle.get(metadataKey.getBytes())
    value != null
  }

  def getMetadata(
      version: Option[Long] = None
  )(defvalue: DeltaStoreMetadata = null): DeltaStoreMetadata = {
    val value = dbHandle.get(metadataKey.getBytes())
    if (value != null) {
      JsonHelper.fromJson[DeltaStoreMetadata](new String(value))
    } else { defvalue }
  }

  def putMetadata(
      version: Option[Long] = None
  )(value: DeltaStoreMetadata): Unit = {
    dbHandle.put(
      metadataKey.getBytes(),
      JsonHelper.toJson[DeltaStoreMetadata](value).getBytes()
    )
  }

  def getAction(
      version: Option[Long] = None
  )(key: String, defvalue: DeltaStoreAddFile = null): DeltaStoreAddFile = {
    val value = dbHandle.get(key.getBytes())
    if (value != null) {
      JsonHelper.fromJson[DeltaStoreAddFile](new String(value))
    } else { defvalue }
  }

  def putAction(
      version: Option[Long] = None
  )(key: String, value: DeltaStoreAddFile): Unit = {
    dbHandle.put(
      key.getBytes(),
      JsonHelper.toJson[DeltaStoreAddFile](value).getBytes()
    )
  }

  def deleteAction(version: Option[Long] = None)(key: String): Unit = {
    dbHandle.delete(key.getBytes())
  }

  def getActions(
      version: Option[Long] = None
  )(prefix: String): Seq[DeltaStoreAddFile] = {
    val items = new ListBuffer[DeltaStoreAddFile]()

    val options = new ReadOptions()
    options.setTailing(true)

    val iter = dbHandle.newIterator(options)
    iter.seek(prefix.getBytes())

    val loop = new Breaks
    loop.breakable {
      while (iter.isValid()) {
        val key = new String(iter.key)

        if (key.startsWith(prefix)) {
          val value = new String(iter.value)

          items += JsonHelper.fromJson[DeltaStoreAddFile](value)
          iter.next()
        } else {
          loop.break
        }
      }
    }

    options.close()

    items
  }

  def getActionsRange(
      version: Option[Long] = None
  )(from: String, to: String): Seq[DeltaStoreAddFile] = {
    val items = new ListBuffer[DeltaStoreAddFile]()

    val options = new ReadOptions()
    options.setTailing(true)

    val iter = dbHandle.newIterator(options)
    iter.seek(from.getBytes())

    if (to.length > 0) {
      val loop = new Breaks
      loop.breakable {
        while (iter.isValid()) {
          val key = new String(iter.key)
          if (!key.startsWith(metadataPrefixKey)) {
            if (key.substring(0, to.length) > to) { loop.break }

            val value = new String(iter.value)

            items += JsonHelper.fromJson[DeltaStoreAddFile](value)
          }
          iter.next()
        }
      }
    } else {
      while (iter.isValid()) {
        val key = new String(iter.key)

        if (!key.startsWith(metadataPrefixKey)) {
          val value = new String(iter.value)

          items += JsonHelper.fromJson[DeltaStoreAddFile](value)
        }
        iter.next()
      }
    }

    options.close()

    items
  }

  def getActionsAll(version: Option[Long] = None)(): Seq[DeltaStoreAddFile] = {
    val items = new ListBuffer[DeltaStoreAddFile]()

    val options = new ReadOptions()
    options.setTailing(true)

    val iter = dbHandle.newIterator(options)
    iter.seekToFirst()

    while (iter.isValid()) {
      val key = new String(iter.key)

      if (!key.startsWith(metadataPrefixKey)) {
        val value = new String(iter.value)

        items += JsonHelper.fromJson[DeltaStoreAddFile](value)
      }
      iter.next()
    }

    options.close()

    items
  }
}
