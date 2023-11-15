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

import java.io.{File, IOException}
import java.util.Collections

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.{
  DefaultScalaModule,
  ScalaObjectMapper
}

trait ConfigBase {
  def check(): Unit
}

case class DeltaSimpleConfig(
    key: String = null,
    value: String = null
) extends ConfigBase {
  def check(): Unit = {}
}

case class DeltaStoreConfig(
    version: Int = 0,
    host: String = "localhost",
    port: Int = 80,
    endpoint: String = "/deltastore",
    preSignedUrlTimeoutSeconds: Long = 3600,
    requestTimeoutSeconds: Long = 30,
    configCacheDuration: Int = 5,
    configCacheSize: Int = 10,
    evaluatePartitionPrefix: Boolean = true,
    evaluatePredicateHints: Boolean = true,
    filesystems: Seq[DeltaFilesystemConfig] = Seq.empty[DeltaFilesystemConfig],
    shares: Seq[DeltaShareConfig] = Seq.empty[DeltaShareConfig],
    repository: DeltaRepositoryConfig = DeltaRepositoryConfig(),
    authentication: DeltaAuthenticationConfig = DeltaAuthenticationConfig(),
    authorization: DeltaAuthorizationConfig = DeltaAuthorizationConfig(),
    configuration: DeltaConfigurationConfig = DeltaConfigurationConfig(),
    logger: DeltaLoggerConfig = DeltaLoggerConfig()
) extends ConfigBase {
  override def check(): Unit = {
    filesystems.foreach(_.check())
    shares.foreach(_.check())
    repository.check()
    authentication.check()
    configuration.check()
    authorization.check()
    logger.check()
  }
}

object DeltaStoreConfig {
  private lazy val mapper = {
    val mapper = new ObjectMapper(new YAMLFactory) with ScalaObjectMapper
    mapper.setSerializationInclusion(Include.NON_ABSENT)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  def save(path: String, config: DeltaStoreConfig): Unit = {
    mapper.writeValue(new File(path), config)
  }

  def load(path: String): DeltaStoreConfig = {
    val config = mapper.readValue(new File(path), classOf[DeltaStoreConfig])
    config.check()
    config
  }
}

case class DeltaFilesystemConfig(
    name: String = null,
    configs: Seq[DeltaSimpleConfig] = Seq.empty[DeltaSimpleConfig]
) extends ConfigBase {
  override def check(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException("'name' in a share must be provided")
    }
  }
}

case class DeltaShareConfig(
    name: String = null,
    schemas: Seq[DeltaSchemaConfig] = Seq.empty[DeltaSchemaConfig]
) extends ConfigBase {
  override def check(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException("'name' in a share must be provided")
    }
    schemas.foreach(_.check())
  }
}

case class DeltaSchemaConfig(
    name: String = null,
    share: String = null,
    tables: Seq[DeltaTableConfig] = Seq.empty[DeltaTableConfig]
) extends ConfigBase {
  override def check(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException("'name' in a schema must be provided")
    }
    tables.foreach(_.check())
  }
}

case class DeltaTableConfig(
    name: String = null,
    share: String = null,
    schema: String = null,
    filesystem: String = null,
    repository: String = null,
    location: String = null,
    versioning: Boolean = true
) extends ConfigBase {
  lazy val id: String = filesystem + "#" + location

  override def check(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException("'name' in a table must be provided")
    }
    if (filesystem == null) {
      throw new IllegalArgumentException(
        "'filesystem' in a table must be provided"
      )
    }
    if (location == null) {
      throw new IllegalArgumentException(
        "'location' in a table must be provided"
      )
    }
  }
}

case class DeltaEngineConfig(
    name: String = "none",
    configs: Seq[DeltaSimpleConfig] = Seq.empty[DeltaSimpleConfig]
) extends ConfigBase {
  override def check(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException(
        "'name' in a engine must be provided"
      )
    }
  }
}

case class DeltaRepositoryConfig(
    name: String = "DB",
    configs: Seq[DeltaSimpleConfig] = Seq.empty[DeltaSimpleConfig],
    engines: Seq[DeltaEngineConfig] = Seq.empty[DeltaEngineConfig]
) extends ConfigBase {
  override def check(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException(
        "'name' in a repository must be provided"
      )
    }
    if (engines.length < 1) {
      throw new IllegalArgumentException(
        "'engines' in a repository must be provided at least one"
      )
    }
    engines.foreach(_.check())
  }
}

case class DeltaAuthenticationConfig(
    name: String = "none",
    configs: Seq[DeltaSimpleConfig] = Seq.empty[DeltaSimpleConfig]
) extends ConfigBase {
  override def check(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException(
        "'name' in a authentication must be provided"
      )
    }
  }
}

case class DeltaConfigurationConfig(
    name: String = "none",
    configs: Seq[DeltaSimpleConfig] = Seq.empty[DeltaSimpleConfig]
) extends ConfigBase {
  override def check(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException(
        "'name' in a configuration must be provided"
      )
    }
  }
}

case class DeltaAuthorizationConfig(
    name: String = "none",
    configs: Seq[DeltaSimpleConfig] = Seq.empty[DeltaSimpleConfig]
) extends ConfigBase {
  override def check(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException(
        "'name' in a authorization must be provided"
      )
    }
  }
}

case class DeltaLoggerConfig(
    maxFoundFilesDump: Int = 10
) extends ConfigBase {
  override def check(): Unit = {}
}
