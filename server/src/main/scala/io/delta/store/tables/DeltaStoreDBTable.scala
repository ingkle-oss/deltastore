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

package io.delta.store.tables

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import scala.collection.JavaConverters._
import scala.collection.parallel.ExecutionContextTaskSupport
import scala.collection.parallel.immutable.ParVector
import scala.concurrent.ExecutionContext

import com.github.mjakubowski84.parquet4s.ParquetReader
import com.google.common.hash.Hashing
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.slf4j.LoggerFactory

import io.delta.store.{
  DeltaEngineConfig,
  DeltaSharingIllegalArgumentException,
  DeltaStoreAddFile,
  DeltaStoreConfig,
  DeltaStoreConfigManager,
  DeltaStoreEngine,
  DeltaStoreEngineBatch,
  DeltaStoreFormat,
  DeltaStoreMetadata,
  DeltaStoreProtocol,
  DeltaStoreTable,
  DeltaTableConfig
}
import io.delta.store.cores.{DeltaActions, DeltaCheckpoints, DeltaModels}
import io.delta.store.engines._
import io.delta.store.helpers._
import io.delta.store.storages.LogStoreProvider

class DeltaStoreDBTable(
    tableConfig: DeltaTableConfig,
    storeConfig: DeltaStoreConfig
) extends DeltaStoreTable(
      tableConfig: DeltaTableConfig,
      storeConfig: DeltaStoreConfig
    )
    with LogStoreProvider {
  private val logger = LoggerFactory.getLogger(classOf[DeltaStoreDBTable])

  private val fileSigner = getFileSigner(new URI(tableConfig.location))

  private val store = createLogStore(fsConfig)

  private val logPath = new Path(tableConfig.location, "_delta_log")

  private val name = new String(
    Base64.getUrlEncoder().encode(tableConfig.id.getBytes(UTF_8)),
    UTF_8
  )

  private val enableCacheSync = {
    storeConfig.repository.configs.find(c => c.key == "enableCacheSync") match {
      case Some(c) => c.value.toBoolean
      case None    => true
    }
  }

  private val enableAutoFlush = {
    storeConfig.repository.configs.find(c => c.key == "enableAutoFlush") match {
      case Some(c) => c.value.toBoolean
      case None    => false
    }
  }

  private val engineConfig = {
    if (tableConfig.repository != null) {
      storeConfig.repository.engines.find(c =>
        c.name == tableConfig.repository
      ) match {
        case Some(c) => c
        case None =>
          throw new DeltaSharingIllegalArgumentException(
            "Invalid repository engine"
          )
      }
    } else {
      storeConfig.repository.engines(0)
    }
  }

  private val maxWriteBatchSize = {
    engineConfig.configs.find(c => c.key == "maxWriteBatchSize") match {
      case Some(c) => c.value.toInt
      case None    => 100000
    }
  }

  private val engine = {
    val engine = if (tableConfig.versioning) {
      engineConfig.configs.find(c => c.key == "engine") match {
        case Some(c) =>
          c.value.toUpperCase() match {
            case _ => new DeltaStoreRocksDBVersioningEngine(engineConfig, name)
          }
        case None => new DeltaStoreRocksDBVersioningEngine(engineConfig, name)
      }
    } else {
      engineConfig.configs.find(c => c.key == "engine") match {
        case Some(c) =>
          c.value.toUpperCase() match {
            case _ => new DeltaStoreRocksDBEngine(engineConfig, name)
          }
        case None => new DeltaStoreRocksDBEngine(engineConfig, name)
      }
    }
    engine
  }

  private def fetchCheckpoints(version: Long): Seq[Path] = {
    store
      .listFrom(FileNames.checkpointPrefix(logPath, version), fsConfig)
      .asScala
      .filter(f => FileNames.isCheckpointFile(f.getPath))
      .toArray
      .map(f => f.getPath)
  }

  private def fetchCommits(version: Long): Seq[Path] = {
    store
      .listFrom(FileNames.deltaFile(logPath, version), fsConfig)
      .asScala
      .filter(f => FileNames.isDeltaFile(f.getPath))
      .toArray
      .map(f => f.getPath)
  }

  private def loadAndPlayCheckpoints(files: Seq[Path]): Unit = {
    val execContextService = ExecutionContext.fromExecutorService(null)

    try {
      val pv = new ParVector(files.map(_.toString).sortWith(_ < _).toVector)
      pv.tasksupport = new ExecutionContextTaskSupport(execContextService)
      pv.foreach { path =>
        val parquetIterable =
          ParquetReader.read[DeltaActions.Parquet4sSingleActionWrapper](
            path,
            ParquetReader.Options(hadoopConf = fsConfig)
          )
        try {
          val groups = parquetIterable.grouped(maxWriteBatchSize)
          while (groups.hasNext) {
            val actions = groups.next().toArray.map(_.unwrap)
            playActions(0)(actions)
          }
        } finally {
          parquetIterable.close()
        }
      }
    } finally {
      execContextService.shutdown()
    }
  }

  private def loadAndPlayCommits(files: Seq[Path]): Unit = {
    val execContextService = ExecutionContext.fromExecutorService(null)

    try {
      val pv = new ParVector(files.map(_.toString).sortWith(_ < _).toVector)
      pv.tasksupport = new ExecutionContextTaskSupport(execContextService)
      val items = pv.map { filepath =>
        val path = new Path(filepath)
        val actions = store
          .read(path, fsConfig)
          .asScala
          .toArray
          .map { line => JsonHelper.fromJson[DeltaActions.SingleAction](line) }
        (path -> actions)
      }.toList
      for ((path, actions) <- items) {
        val version = FileNames.deltaVersion(path)
        playActions(version)(actions)
      }
    } finally {
      execContextService.shutdown()
    }
  }

  private def playActions(
      version: Long
  )(actions: Seq[DeltaActions.SingleAction]): Unit = {
    val batch = engine.getBatch()

    actions.map(_.unwrap).foreach {
      case add: DeltaActions.AddFile =>
        batch.putAction(Some(version))(
          add.path,
          DeltaStoreAddFile(
            path = add.path,
            partitionValues = add.partitionValues,
            size = add.size,
            stats = add.stats
          )
        )
      case protocol: DeltaActions.Protocol =>
        batch.putProtocol(Some(version))(
          DeltaStoreProtocol(protocol.minReaderVersion)
        )
      case metadata: DeltaActions.Metadata =>
        batch.putMetadata(Some(version))(
          DeltaStoreMetadata(
            id = metadata.id,
            name = metadata.name,
            description = metadata.description,
            format = DeltaStoreFormat(),
            schemaString = cleanTableSchema(metadata.schemaString),
            configuration = getMetadataConfiguration(metadata.configuration),
            partitionColumns = metadata.partitionColumns
          )
        )
      case _ => // do nothing
    }

    engine.putBatch(batch)

    actions.map(_.unwrap).foreach {
      case remove: DeltaActions.RemoveFile =>
        engine.deleteAction(Some(version))(remove.path)
      case _ => // do nothing
    }
  }

  private def flushActions(): Unit = {
    engine.flush()
  }

  override def update(): Unit = {
    this.synchronized {
      var needsFlush = false

      if (!engine.hasLatestCommit()) {
        try {
          // db is empty, build from last checkpoint
          logger.info(s"Updating logs from _last_checkpoint")
          val lastCheckpointPath = new Path(logPath, "_last_checkpoint")
          val checkpointMetadataJson = store.read(lastCheckpointPath, fsConfig)
          val checkpointMetadata =
            JsonHelper.fromJson[DeltaCheckpoints.CheckpointMetaData](
              checkpointMetadataJson.next()
            )
          logger.info(
            s"Updating logs from ${checkpointMetadata.version} version"
          )
          val checkpoints = fetchCheckpoints(checkpointMetadata.version)
          loadAndPlayCheckpoints(checkpoints)
          engine.putLatestCommit(checkpointMetadata.version)
          needsFlush = true
          logger.info(
            s"Done updating the latest commit version ${checkpointMetadata.version}"
          )
        } catch {
          case e: FileNotFoundException => logger.info("No _last_checkpoint")
          case e: Exception             => throw e
        }
      }

      val commitVersion = engine.getLatestCommit(-1)

      // check if new logs exist
      logger.info(s"Updating logs from ${commitVersion + 1} version")
      val commits = fetchCommits(commitVersion + 1)
      if (commits.length > 0) {
        val newCommitVersion = FileNames.deltaVersion(commits.last)

        loadAndPlayCommits(commits)
        engine.putLatestCommit(newCommitVersion)
        needsFlush = true

        logger.info(
          s"Done updating the lastest commit version ${newCommitVersion}"
        )
      }

      if (enableAutoFlush && needsFlush) {
        logger.info(s"Flush all logs")

        flushActions()
      }
    }
  }

  override def query(filters: Seq[String] = Nil)(
      includeFiles: Boolean,
      predicateHints: Seq[String],
      limitHint: Option[Long],
      version: Option[Long]
  ): (Long, Seq[DeltaModels.SingleAction]) = withClassLoader {
    logger.info(
      s"query(filters = ${filters})"
        + s"(includeFiles = ${includeFiles}, predicateHints = ${predicateHints}, "
        + s"limitHint = ${limitHint}, version = ${version})"
    )

    if (enableCacheSync && !engine.hasMetadata(version)()) { update() }

    val metadata = engine.getMetadata(version)()
    val protocol = engine.getProtocol(version)()
    val commitVersion = engine.getLatestCommit()

    val actions = Seq(
      DeltaModels.Protocol(
        minReaderVersion = protocol.minReaderVersion
      ),
      DeltaModels.Metadata(
        id = metadata.id,
        name = metadata.name,
        description = metadata.description,
        format = DeltaModels.Format(metadata.format.provider),
        schemaString = metadata.schemaString,
        configuration = metadata.configuration,
        partitionColumns = metadata.partitionColumns
      )
    ) ++ {
      if (includeFiles) {
        val tableSchema = DataType
          .fromJson(metadata.schemaString)
          .asInstanceOf[StructType]
        val partitionColumns = metadata.partitionColumns
        val partitionSchema = new StructType(
          partitionColumns.map(c => tableSchema(c)).toArray
        )
        val dataSchema = new StructType(
          tableSchema.names
            .filterNot(n => partitionColumns.contains(n))
            .map(c => tableSchema(c))
            .toArray
        )

        val time0 = System.currentTimeMillis()

        val selectedFiles = {
          if (storeConfig.evaluatePartitionPrefix) {
            val (prefixFrom, prefixTo) =
              PartitionPrefixHelper.getPrefix(
                partitionColumns,
                predicateHints
              )

            if (prefixFrom.length == 0 && prefixTo.length == 0) {
              logger.info(s"Seek all")

              engine.getActionsAll(version)()
            } else if (prefixFrom == prefixTo) {
              logger.info(s"Seek on [${prefixFrom}]")

              engine.getActions(version)(prefixFrom)
            } else {
              logger.info(s"Seek from [${prefixFrom}] to [${prefixTo}]")

              engine.getActionsRange(version)(prefixFrom, prefixTo)
            }
          } else {
            logger.info(s"Seek all")

            engine.getActionsAll(version)()
          }
        }

        logger.info(
          s"Found selected ${selectedFiles.length} files [${System.currentTimeMillis() - time0}]"
        )

        val partitionedFiles =
          if (storeConfig.evaluatePredicateHints && partitionColumns.nonEmpty) {
            PartitionFilterHelper.evaluatePredicate(
              partitionSchema,
              predicateHints,
              selectedFiles
            )
          } else {
            selectedFiles
          }
        val accessableFiles =
          if (filters.length > 0) {
            PartitionFilterHelper.evaluatePredicate(
              partitionSchema,
              filters,
              partitionedFiles
            )
          } else {
            partitionedFiles
          }
        val filteredFiles =
          if (storeConfig.evaluatePredicateHints) {
            DataFilterHelper.evaluatePredicate(
              dataSchema,
              predicateHints,
              accessableFiles
            )
          } else {
            accessableFiles
          }

        if (storeConfig.logger.maxFoundFilesDump > 0) {
          filteredFiles.take(storeConfig.logger.maxFoundFilesDump).foreach {
            addFile =>
              logger.info(
                s"Found ${addFile.path} file (${addFile.size / 1024} kbytes)"
              )
          }
        }

        val files = filteredFiles.map { addFile =>
          val cloudPath =
            getAbsolutePath(new Path(tableConfig.location), addFile.path)
          val signedUrl = fileSigner.sign(cloudPath)
          val action = DeltaModels.AddFile(
            url = signedUrl,
            id = Hashing.md5().hashString(addFile.path, UTF_8).toString,
            partitionValues = addFile.partitionValues,
            size = addFile.size,
            stats = addFile.stats
          )
          action
        }

        logger.info(
          s"Found filtered ${files.length} files [${System.currentTimeMillis() - time0}]"
        )

        files
      } else {
        Nil
      }
    }

    commitVersion -> actions.map(action => action.wrap)
  }
}
