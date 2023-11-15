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

import java.net.{URL, URLEncoder}
import java.util.Base64

import scala.collection.JavaConverters._

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.http.client.methods.{
  CloseableHttpResponse,
  HttpGet,
  HttpHead,
  HttpPost
}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.params.HttpParams
import org.slf4j.LoggerFactory

import io.delta.store.{
  DeltaFilesystemConfig,
  DeltaSchemaConfig,
  DeltaShareConfig,
  DeltaStoreConfig,
  DeltaStoreConfigManager,
  DeltaTableConfig
}
import io.delta.store.helpers._

case class DeltaStoreConfigListSharesResponse(
    items: Seq[DeltaShareConfig],
    token: String
)

case class DeltaStoreConfigListSchemasResponse(
    items: Seq[DeltaSchemaConfig],
    token: String
)

case class DeltaStoreConfigListTablesResponse(
    items: Seq[DeltaTableConfig],
    token: String
)

class DeltaStoreConfigServerManager(storeConfig: DeltaStoreConfig)
    extends DeltaStoreConfigManager(storeConfig: DeltaStoreConfig) {
  private val logger =
    LoggerFactory.getLogger(classOf[DeltaStoreConfigServerManager])

  private lazy val serverEndpoint = {
    storeConfig.configuration.configs.find(c => c.key == "endpoint") match {
      case Some(c) => c.value
      case None    => ""
    }
  }

  private lazy val serverUsername = {
    storeConfig.configuration.configs.find(c => c.key == "username") match {
      case Some(c) => c.value
      case None    => ""
    }
  }

  private lazy val serverPassword = {
    storeConfig.configuration.configs.find(c => c.key == "password") match {
      case Some(c) => c.value
      case None    => ""
    }
  }

  private lazy val timeoutInSeconds = {
    storeConfig.configuration.configs.find(c => c.key == "timeout") match {
      case Some(c) => c.value.toInt
      case None    => 120
    }
  }

  private lazy val serverHost = HttpHelper.getHttpHost(serverEndpoint)
  private lazy val httpClient = HttpHelper.getHttpClient(timeoutInSeconds)

  private lazy val serverToken = Base64.getUrlEncoder.encodeToString(
    (serverUsername + ":" + serverPassword).getBytes()
  )

  protected override def getFilesystemConfigLoader()
      : CacheLoader[String, DeltaFilesystemConfig] = {
    new CacheLoader[String, DeltaFilesystemConfig]() {
      def load(key: String): DeltaFilesystemConfig = {
        val fsName = key

        val body = handleRequest(s"/filesystems/${fsName}")
        val fsConfig = JsonHelper.fromJson[DeltaFilesystemConfig](body)
        fsConfig
      }
    }
  }

  protected override def getShareConfigLoader()
      : CacheLoader[String, DeltaShareConfig] = {
    new CacheLoader[String, DeltaShareConfig]() {
      def load(key: String): DeltaShareConfig = {
        val shareName = key

        val body = handleRequest(s"/shares/${shareName}")
        val shareConfig = JsonHelper.fromJson[DeltaShareConfig](body)
        shareConfig
      }
    }
  }

  protected override def getSchemaConfigLoader()
      : CacheLoader[String, DeltaSchemaConfig] = {
    new CacheLoader[String, DeltaSchemaConfig]() {
      def load(key: String): DeltaSchemaConfig = {
        val items = key.split('.')
        val shareName = items(0)
        val schemaName = items(1)

        val body = handleRequest(
          s"/shares/${shareName}/schemas/${schemaName}"
        )
        val schemaConfig = JsonHelper.fromJson[DeltaSchemaConfig](body)
        schemaConfig
      }
    }
  }

  protected override def getTableConfigLoader()
      : CacheLoader[String, DeltaTableConfig] = {
    new CacheLoader[String, DeltaTableConfig]() {
      def load(key: String): DeltaTableConfig = {
        val items = key.split('.')
        val shareName = items(0)
        val schemaName = items(1)
        val tableName = items(2)

        val body = handleRequest(
          s"/shares/${shareName}/schemas/${schemaName}/tables/${tableName}"
        )
        val tableConfig = JsonHelper.fromJson[DeltaTableConfig](body)
        tableConfig
      }
    }
  }

  override def listShares(
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaShareConfig], Option[String]) = {
    val body = handleRequest(
      s"/shares", {
        if (nextToken.isDefined) { Map("token" -> nextToken.get) }
        else { Map() }
      }
    )
    val shares = JsonHelper.fromJson[DeltaStoreConfigListSharesResponse](body)
    if (shares.token == null) { shares.items -> None }
    else { shares.items -> Some(shares.token) }
  }

  override def listSchemas(
      shareName: String,
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaSchemaConfig], Option[String]) = {
    val body = handleRequest(
      s"/shares/${shareName}/schemas", {
        if (nextToken.isDefined) { Map("token" -> nextToken.get) }
        else { Map() }
      }
    )
    val schemas =
      JsonHelper.fromJson[DeltaStoreConfigListSchemasResponse](body)
    if (schemas.token == null) { schemas.items -> None }
    else { schemas.items -> Some(schemas.token) }
  }

  override def listTables(
      shareName: String,
      schemaName: String,
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaTableConfig], Option[String]) = {
    val body = handleRequest(
      s"/shares/${shareName}/schemas/${schemaName}/tables", {
        if (nextToken.isDefined) { Map("token" -> nextToken.get) }
        else { Map() }
      }
    )
    val tables =
      JsonHelper.fromJson[DeltaStoreConfigListTablesResponse](body)
    if (tables.token == null) { tables.items -> None }
    else { tables.items -> Some(tables.token) }
  }

  override def listAllTables(
      shareName: String,
      nextToken: Option[String] = None,
      maxResults: Option[Int] = None
  ): (Seq[DeltaTableConfig], Option[String]) = {
    val body = handleRequest(
      s"/shares/${shareName}/tables", {
        if (nextToken.isDefined) { Map("token" -> nextToken.get) }
        else { Map() }
      }
    )
    val tables =
      JsonHelper.fromJson[DeltaStoreConfigListTablesResponse](body)
    if (tables.token == null) { tables.items -> None }
    else { tables.items -> Some(tables.token) }
  }

  private def handleRequest(
      url: String,
      kvs: Map[String, String] = Map()
  ): String = {
    val req = new HttpGet(s"${serverEndpoint}" + url)
    req.setHeader(HttpHeaders.AUTHORIZATION, s"Basic ${serverToken}")
    if (kvs.size > 0) {
      val params = req.getParams()
      kvs.foreach { kv =>
        params.setParameter(kv._1, kv._2)
      }
      req.setParams(params)
    }
    val res =
      httpClient.execute(serverHost, req, HttpClientContext.create())

    try {
      val status = res.getStatusLine()
      if (status.getStatusCode != HttpStatus.SC_OK) {
        throw new IllegalArgumentException(url)
      }

      val body = HttpHelper.getHttpResponseContent(res)

      body
    } catch {
      case e: Exception => throw new NoSuchElementException(url)
    } finally {
      res.close()
    }
  }
}
