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

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azure.NativeAzureFileSystem
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import org.slf4j.LoggerFactory

import io.delta.store.cores.DeltaModels
import io.delta.store.helpers._

class DeltaStoreTable(
    tableConfig: DeltaTableConfig,
    storeConfig: DeltaStoreConfig
) {
  protected def withClassLoader[T](func: => T): T = {
    val classLoader = Thread.currentThread().getContextClassLoader
    if (classLoader == null) {
      Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
      try func
      finally {
        Thread.currentThread().setContextClassLoader(null)
      }
    } else {
      func
    }
  }

  protected val configManager = DeltaStoreConfigManager()

  protected val fsConfig = withClassLoader {
    val config = new Configuration()
    val filesystem = configManager.getFilesystem(tableConfig.filesystem)
    filesystem.configs.foreach(c => { config.set(c.key, c.value) })
    config
  }

  protected def getFileSigner(uri: URI) = withClassLoader {
    val tablePath = new Path(tableConfig.location)
    val fs = tablePath.getFileSystem(fsConfig)
    fs match {
      case _: S3AFileSystem =>
        new S3FileSigner(
          uri,
          fsConfig,
          storeConfig.preSignedUrlTimeoutSeconds
        )
      case wasb: NativeAzureFileSystem =>
        WasbFileSigner(
          wasb,
          uri,
          fsConfig,
          storeConfig.preSignedUrlTimeoutSeconds
        )
      case abfs: AzureBlobFileSystem =>
        AbfsFileSigner(
          abfs,
          uri,
          storeConfig.preSignedUrlTimeoutSeconds
        )
      case gc: GoogleHadoopFileSystem =>
        new GCSFileSigner(
          uri,
          fsConfig,
          storeConfig.preSignedUrlTimeoutSeconds
        )
      case _ =>
        throw new IllegalStateException(
          s"File system ${fs.getClass} is not supported"
        )
    }
  }

  protected def cleanTableSchema(schemaString: String): String = {
    StructType(
      DataType.fromJson(schemaString).asInstanceOf[StructType].map { field =>
        val newMetadata = new MetadataBuilder()
        if (field.metadata.contains("comment")) {
          newMetadata.putString("comment", field.metadata.getString("comment"))
        }
        field.copy(metadata = newMetadata.build())
      }
    ).json
  }

  protected def getMetadataConfiguration(
      tableConf: Map[String, String]
  ): Map[String, String] = {
    if (tableConf.getOrElse("delta.enableChangeDataFeed", "false") == "true") {
      Map("enableChangeDataFeed" -> "true")
    } else {
      Map.empty
    }
  }

  protected def getAbsolutePath(path: Path, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      throw new IllegalStateException(
        "table containing absolute paths cannot be shared"
      )
    } else {
      new Path(path, p)
    }
  }

  def update(): Unit = {}

  def query(filters: Seq[String] = Nil)(
      includeFiles: Boolean,
      predicateHints: Seq[String],
      limitHint: Option[Long],
      version: Option[Long]
  ): (Long, Seq[DeltaModels.SingleAction]) = withClassLoader {
    0.toLong -> Nil
  }
}
