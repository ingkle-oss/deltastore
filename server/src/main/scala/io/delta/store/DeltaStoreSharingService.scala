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

import java.io.{ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.AccessDeniedException
import java.security.MessageDigest
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.Nullable

import scala.collection.JavaConverters._

import com.linecorp.armeria.common.{
  HttpData,
  HttpHeaderNames,
  HttpHeaders,
  HttpMethod,
  HttpRequest,
  HttpResponse,
  HttpStatus,
  MediaType,
  ResponseHeaders,
  ResponseHeadersBuilder
}
import com.linecorp.armeria.common.auth.OAuth2Token
import com.linecorp.armeria.internal.server.ResponseConversionUtil
import com.linecorp.armeria.server.{Server, ServiceRequestContext}
import com.linecorp.armeria.server.annotation.{
  Blocking,
  ConsumesJson,
  Default,
  ExceptionHandler,
  ExceptionHandlerFunction,
  Get,
  Head,
  Param,
  Post,
  ProducesJson
}
import com.linecorp.armeria.server.auth.AuthService
import io.delta.sharing.server.protocol._
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import io.delta.store.cores.DeltaModels
import io.delta.store.helpers.JsonHelper

class DeltaStoreSharingServiceExceptionHandler
    extends ExceptionHandlerFunction {
  private val logger =
    LoggerFactory.getLogger(classOf[DeltaStoreSharingServiceExceptionHandler])

  override def handleException(
      ctx: ServiceRequestContext,
      req: HttpRequest,
      cause: Throwable
  ): HttpResponse = {
    cause match {
      case _: DeltaSharingNoSuchElementException =>
        if (req.method().equals(HttpMethod.HEAD)) {
          HttpResponse.of(ResponseHeaders.builder(HttpStatus.NOT_FOUND).build())
        } else {
          HttpResponse.of(
            HttpStatus.NOT_FOUND,
            MediaType.JSON_UTF_8,
            JsonHelper.toJson(
              Map(
                "errorCode" -> ErrorCode.RESOURCE_DOES_NOT_EXIST,
                "message" -> cause.getMessage
              )
            )
          )
        }
      case _: DeltaSharingIllegalArgumentException =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonHelper.toJson(
            Map(
              "errorCode" -> ErrorCode.INVALID_PARAMETER_VALUE,
              "message" -> cause.getMessage
            )
          )
        )
      case _: AccessDeniedException =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonHelper.toJson(
            Map(
              "errorCode" -> ErrorCode.ACCESS_DENIED,
              "message" -> cause.getMessage
            )
          )
        )
      case (_: scalapb.json4s.JsonFormatException |
          _: com.fasterxml.jackson.databind.JsonMappingException) =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonHelper.toJson(
            Map(
              "errorCode" -> ErrorCode.MALFORMED_REQUEST,
              "message" -> cause.getMessage
            )
          )
        )
      case _: NumberFormatException =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonHelper.toJson(
            Map(
              "errorCode" -> ErrorCode.INVALID_PARAMETER_VALUE,
              "message" -> "expected a number but the string didn't have the appropriate format"
            )
          )
        )
      case _ =>
        logger.error(cause.getMessage, cause)
        HttpResponse.of(
          HttpStatus.INTERNAL_SERVER_ERROR,
          MediaType.PLAIN_TEXT_UTF_8,
          JsonHelper.toJson(
            Map("errorCode" -> ErrorCode.INTERNAL_ERROR, "message" -> "")
          )
        )
    }
  }
}

@Blocking
@ExceptionHandler(classOf[DeltaStoreSharingServiceExceptionHandler])
class DeltaStoreSharingService(storeConfig: DeltaStoreConfig) {
  private val logger =
    LoggerFactory.getLogger(classOf[DeltaStoreSharingService])

  private val DELTA_TABLE_VERSION_HEADER = "Delta-Table-Version"
  private val DELTA_TABLE_METADATA_CONTENT_TYPE =
    "application/x-ndjson; charset=utf-8"

  private val configManager = DeltaStoreConfigManager()
  private val tableManager = DeltaStoreTableManager()
  private val authZManager = DeltaStoreAuthZManager()

  private val totalRequests = new AtomicLong(0)

  private def getNextRequestId(): Long = totalRequests.getAndIncrement()

  @Get("/shares")
  @ProducesJson
  def listShares(
      req: HttpRequest,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String
  ): ListSharesResponse = handleRequestWithToken(req) { userToken: String =>
    logger.info(
      s"listShares(maxResults = ${maxResults}, pageToken = ${pageToken})"
    )

    authZManager.checkListShares(userToken)

    val (shares, nextToken) =
      configManager.listShares(Option(pageToken), Some(maxResults))
    val list = shares.map(s => Share().withName(s.name))

    ListSharesResponse(list, nextToken)
  }

  @Get("/shares/{share}")
  @ProducesJson
  def getShare(
      req: HttpRequest,
      @Param("share") shareName: String
  ): GetShareResponse = handleRequestWithToken(req) { userToken: String =>
    GetShareResponse(null)
  }

  @Get("/shares/{share}/schemas")
  @ProducesJson
  def listSchemas(
      req: HttpRequest,
      @Param("share") shareName: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String
  ): ListSchemasResponse = handleRequestWithToken(req) { userToken: String =>
    logger.info(
      s"listSchemas(share = ${shareName}, maxResults = ${maxResults}, pageToken = ${pageToken})"
    )

    authZManager.checkListSchemas(userToken, shareName)

    val (schemas, nextToken) =
      configManager.listSchemas(shareName, Option(pageToken), Some(maxResults))
    val list = schemas.map(c => Schema().withName(c.name).withShare(c.share))

    ListSchemasResponse(list, nextToken)
  }

  @Get("/shares/{share}/schemas/{schema}/tables")
  @ProducesJson
  def listTables(
      req: HttpRequest,
      @Param("share") shareName: String,
      @Param("schema") schemaName: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String
  ): ListTablesResponse = handleRequestWithToken(req) { userToken: String =>
    logger.info(
      s"listTables(share = ${shareName}, schema = ${schemaName}, "
        + s"maxResults = ${maxResults}, pageToken = ${pageToken})"
    )

    authZManager.checkListTables(userToken, shareName, schemaName)

    val (tables, nextToken) =
      configManager.listTables(
        shareName,
        schemaName,
        Option(pageToken),
        Some(maxResults)
      )
    val list = tables.map(t =>
      Table().withName(t.name).withSchema(t.schema).withShare(t.share)
    )

    ListTablesResponse(list, nextToken)
  }

  @Get("/shares/{share}/all-tables")
  @ProducesJson
  def listAllTables(
      req: HttpRequest,
      @Param("share") shareName: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String
  ): ListAllTablesResponse = handleRequestWithToken(req) { userToken: String =>
    logger.info(
      s"listAllTables(share = ${shareName}, maxResults = ${maxResults}, pageToken = ${pageToken})"
    )

    authZManager.checkListAllTables(userToken, shareName)

    val (tables, nextToken) =
      configManager.listAllTables(
        shareName,
        Option(pageToken),
        Some(maxResults)
      )
    val list = tables.map(t =>
      Table().withName(t.name).withSchema(t.schema).withShare(t.share)
    )

    ListAllTablesResponse(list, nextToken)
  }

  @Head("/shares/{share}/schemas/{schema}/tables/{table}")
  def getTableVersion(
      req: HttpRequest,
      @Param("share") shareName: String,
      @Param("schema") schemaName: String,
      @Param("table") tableName: String
  ): HttpResponse = handleRequestWithToken(req) { userToken: String =>
    logger.info(
      s"getTableVersion(share = ${shareName}, schema = ${schemaName}, table = ${tableName})"
    )

    HttpResponse.of(
      HttpStatus.NOT_FOUND,
      MediaType.JSON_UTF_8,
      JsonHelper.toJson(
        Map(
          "errorCode" -> 404
        )
      )
    )
  }

  @Get("/shares/{share}/schemas/{schema}/tables/{table}/metadata")
  def getMetadata(
      req: HttpRequest,
      @Param("share") shareName: String,
      @Param("schema") schemaName: String,
      @Param("table") tableName: String
  ): HttpResponse = handleRequestWithToken(req) { userToken: String =>
    logger.info(
      s"getMetadata(share = ${shareName}, schema = ${schemaName}, table = ${tableName})"
    )

    val tableConfig = configManager.getTable(shareName, schemaName, tableName)
    if (tableConfig == null) {
      throw new DeltaSharingNoSuchElementException(
        s"/${shareName}/${schemaName}/${tableName}"
      )
    }
    val table = tableManager.getTable(tableConfig)

    val (_version, _actions) = table.query()(
      includeFiles = false,
      predicateHints = Nil,
      limitHint = None,
      version = None
    )

    handleResponse(Some(_version), _actions)
  }

  @Post("/shares/{share}/schemas/{schema}/tables/{table}/query")
  @ConsumesJson
  def listFiles(
      req: HttpRequest,
      @Param("share") shareName: String,
      @Param("schema") schemaName: String,
      @Param("table") tableName: String,
      queryTableRequest: QueryTableRequest
  ): HttpResponse = handleRequestWithToken(req) { userToken: String =>
    logger.info(
      s"listFiles(share = ${shareName}, schema = ${schemaName}, table = ${tableName})"
    )

    val authz =
      authZManager.checkListFiles(userToken, shareName, schemaName, tableName)
    logger.info(s"checkListFiles() = ${authz}")

    val tableConfig = configManager.getTable(shareName, schemaName, tableName)
    if (tableConfig == null) {
      throw new DeltaSharingNoSuchElementException(
        s"/${shareName}/${schemaName}/${tableName}"
      )
    }
    val table = tableManager.getTable(tableConfig)

    val (_version, _actions) = table.query(authz.filters)(
      includeFiles = true,
      predicateHints = queryTableRequest.predicateHints,
      limitHint = queryTableRequest.limitHint,
      version = queryTableRequest.version
    )

    handleResponse(Some(_version), _actions)
  }

  private def createHeadersBuilderForTableVersion(
      version: Long
  ): ResponseHeadersBuilder = {
    ResponseHeaders
      .builder(200)
      .set(DELTA_TABLE_VERSION_HEADER, version.toString)
  }

  private def handleResponse(
      version: Option[Long],
      actions: Seq[DeltaModels.SingleAction]
  ): HttpResponse = {
    val headers = if (version.isDefined) {
      createHeadersBuilderForTableVersion(version.get)
        .set(HttpHeaderNames.CONTENT_TYPE, DELTA_TABLE_METADATA_CONTENT_TYPE)
        .build()
    } else {
      ResponseHeaders
        .builder(200)
        .set(HttpHeaderNames.CONTENT_TYPE, DELTA_TABLE_METADATA_CONTENT_TYPE)
        .build()
    }
    ResponseConversionUtil.streamingFrom(
      actions.asJava.stream(),
      headers,
      HttpHeaders.of(),
      (o: DeltaModels.SingleAction) => {
        val out = new ByteArrayOutputStream
        JsonHelper.toJson[DeltaModels.SingleAction](out, o)
        out.write('\n')
        HttpData.wrap(out.toByteArray)
      },
      ServiceRequestContext.current().blockingTaskExecutor()
    )
  }

  private def handleRequest[T](req: HttpRequest)(func: => T): T = {
    try {
      val requestId = getNextRequestId()

      logger.info(f"$requestId%012d API call")

      val time0 = System.currentTimeMillis()
      val res = func
      logger.info(
        f"$requestId%012d API done [${System.currentTimeMillis() - time0}]"
      )

      res
    } catch {
      case e: DeltaSharingNoSuchElementException   => throw e
      case e: DeltaSharingIllegalArgumentException => throw e
      case e: AccessDeniedException                => throw e
      case e: Throwable => throw new DeltaInternalException(e)
    }
  }

  private def handleRequestWithToken[T](
      req: HttpRequest
  )(func: String => T): T = {
    try {
      val requestId = getNextRequestId()
      val userToken = req.headers().get("authorization")

      logger.info(f"$requestId%012d API call")

      val time0 = System.currentTimeMillis()
      val res = func(userToken)
      logger.info(
        f"$requestId%012d API done [${System.currentTimeMillis() - time0}]"
      )

      res
    } catch {
      case e: DeltaSharingNoSuchElementException   => throw e
      case e: DeltaSharingIllegalArgumentException => throw e
      case e: AccessDeniedException                => throw e
      case e: Throwable => throw new DeltaInternalException(e)
    }
  }
}
