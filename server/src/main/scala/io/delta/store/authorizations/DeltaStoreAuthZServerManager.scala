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

package io.delta.store.authorizations

import java.io.IOException
import java.nio.file.AccessDeniedException
import java.util.Base64

import scala.collection.JavaConverters._

import org.apache.http.{HttpHeaders, HttpHost, HttpStatus}
import org.apache.http.client.methods.{HttpGet, HttpHead, HttpPost}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.slf4j.LoggerFactory

import io.delta.store.{
  DeltaStoreAuthZListFiles,
  DeltaStoreAuthZManager,
  DeltaStoreConfig
}
import io.delta.store.helpers._

sealed abstract class DeltaStoreAuthZRequest(token: String) {}
sealed abstract class DeltaStoreAuthZResponse(success: Boolean, reason: String)

case class DeltaStoreAuthZListSharesRequest(token: String)
    extends DeltaStoreAuthZRequest(token)

case class DeltaStoreAuthZListSharesResponse(success: Boolean, reason: String)
    extends DeltaStoreAuthZResponse(success, reason)

case class DeltaStoreAuthZListSchemasRequest(token: String, share: String)
    extends DeltaStoreAuthZRequest(token)

case class DeltaStoreAuthZListSchemasResponse(success: Boolean, reason: String)
    extends DeltaStoreAuthZResponse(success, reason)

case class DeltaStoreAuthZListTablesRequest(
    token: String,
    share: String,
    schema: String
) extends DeltaStoreAuthZRequest(token)

case class DeltaStoreAuthZListTablesResponse(success: Boolean, reason: String)
    extends DeltaStoreAuthZResponse(success, reason)

case class DeltaStoreAuthZListAllTablesRequest(token: String, share: String)
    extends DeltaStoreAuthZRequest(token)

case class DeltaStoreAuthZListAllTablesResponse(
    success: Boolean,
    reason: String
) extends DeltaStoreAuthZResponse(success, reason)

case class DeltaStoreAuthZListFilesRequest(
    token: String,
    share: String,
    schema: String,
    table: String
) extends DeltaStoreAuthZRequest(token)

case class DeltaStoreAuthZListFilesResponse(
    success: Boolean,
    reason: String,
    filters: Seq[String]
) extends DeltaStoreAuthZResponse(success, reason)

class DeltaStoreAuthZServerManager(storeConfig: DeltaStoreConfig)
    extends DeltaStoreAuthZManager(storeConfig: DeltaStoreConfig) {
  private val logger =
    LoggerFactory.getLogger(classOf[DeltaStoreAuthZServerManager])

  private lazy val serverEndpoint = {
    storeConfig.authorization.configs.find(c => c.key == "endpoint") match {
      case Some(c) => c.value
      case None    => ""
    }
  }

  private lazy val serverUsername = {
    storeConfig.authorization.configs.find(c => c.key == "username") match {
      case Some(c) => c.value
      case None    => ""
    }
  }

  private lazy val serverPassword = {
    storeConfig.authorization.configs.find(c => c.key == "password") match {
      case Some(c) => c.value
      case None    => ""
    }
  }

  private lazy val timeoutInSeconds = {
    storeConfig.authorization.configs.find(c => c.key == "timeout") match {
      case Some(c) => c.value.toInt
      case None    => 120
    }
  }

  private lazy val serverHost = HttpHelper.getHttpHost(serverEndpoint)
  private lazy val httpClient = HttpHelper.getHttpClient(timeoutInSeconds)

  private lazy val serverToken = Base64.getUrlEncoder.encodeToString(
    (serverUsername + ":" + serverPassword).getBytes()
  )

  override def checkListShares(token: String): Unit = {
    val body = handleRequest(
      JsonHelper.toJson[DeltaStoreAuthZListSharesRequest](
        DeltaStoreAuthZListSharesRequest(token = token)
      ),
      "/list-shares"
    )
    val response = JsonHelper.fromJson[DeltaStoreAuthZListSharesResponse](body)
    if (!response.success) {
      throw new AccessDeniedException(response.reason)
    }
  }

  override def checkListSchemas(token: String, share: String): Unit = {
    val body = handleRequest(
      JsonHelper.toJson[DeltaStoreAuthZListSchemasRequest](
        DeltaStoreAuthZListSchemasRequest(token = token, share = share)
      ),
      "/list-schemas"
    )
    val response = JsonHelper.fromJson[DeltaStoreAuthZListSchemasResponse](body)
    if (!response.success) {
      throw new AccessDeniedException(response.reason)
    }
  }

  override def checkListTables(
      token: String,
      share: String,
      schema: String
  ): Unit = {
    val body = handleRequest(
      JsonHelper.toJson[DeltaStoreAuthZListTablesRequest](
        DeltaStoreAuthZListTablesRequest(
          token = token,
          share = share,
          schema = schema
        )
      ),
      "/list-tables"
    )
    val response = JsonHelper.fromJson[DeltaStoreAuthZListTablesResponse](body)
    if (!response.success) {
      throw new AccessDeniedException(response.reason)
    }
  }

  override def checkListAllTables(token: String, share: String): Unit = {
    val body = handleRequest(
      JsonHelper.toJson[DeltaStoreAuthZListAllTablesRequest](
        DeltaStoreAuthZListAllTablesRequest(
          token = token,
          share = share
        )
      ),
      "/list-all-tables"
    )
    val response =
      JsonHelper.fromJson[DeltaStoreAuthZListAllTablesResponse](body)
    if (!response.success) {
      throw new AccessDeniedException(response.reason)
    }
  }

  override def checkListFiles(
      token: String,
      share: String,
      schema: String,
      table: String
  ): DeltaStoreAuthZListFiles = {
    val body = handleRequest(
      JsonHelper.toJson[DeltaStoreAuthZListFilesRequest](
        DeltaStoreAuthZListFilesRequest(
          token = token,
          share = share,
          schema = schema,
          table = table
        )
      ),
      "/list-files"
    )
    val response = JsonHelper.fromJson[DeltaStoreAuthZListFilesResponse](body)
    if (!response.success) {
      throw new AccessDeniedException(response.reason)
    }
    DeltaStoreAuthZListFiles(filters = response.filters)
  }

  private def handleRequest(entity: String, resource: String): String = {
    val req = new HttpPost(serverEndpoint + resource)
    req.setHeader(HttpHeaders.AUTHORIZATION, s"Basic ${serverToken}")
    req.setEntity(new StringEntity(entity, ContentType.APPLICATION_JSON));
    val res = httpClient.execute(serverHost, req, HttpClientContext.create())

    try {
      val status = res.getStatusLine()
      if (status.getStatusCode != HttpStatus.SC_OK) {
        throw new IOException(
          s"Failed to check on ${resource}: "
            + s"response code = ${status.getStatusCode()}, message = ${status}"
        );
      }

      val body = HttpHelper.getHttpResponseContent(res)

      body
    } finally {
      res.close()
    }
  }
}
