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

package io.delta.store.helpers

import java.net.{URL, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.commons.io.IOUtils
import org.apache.http.{HttpHeaders, HttpHost, HttpStatus}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{
  CloseableHttpResponse,
  HttpGet,
  HttpHead,
  HttpPost,
  HttpRequestBase
}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.conn.ssl.{
  SSLConnectionSocketFactory,
  SSLContextBuilder,
  TrustSelfSignedStrategy
}
import org.apache.http.impl.client.{
  CloseableHttpClient,
  HttpClientBuilder,
  HttpClients
}

object HttpHelper {
  def getHttpHost(endpoint: String): HttpHost = {
    val url = new URL(endpoint)
    val protocol = url.getProtocol
    val port = if (url.getPort == -1) {
      if (protocol == "https") 443 else 80
    } else {
      url.getPort
    }
    new HttpHost(url.getHost, port, protocol)
  }

  def getHttpClient(
      timeoutInSeconds: Int = 120,
      sslTrustAll: Boolean = true
  ): CloseableHttpClient = {
    val clientBuilder: HttpClientBuilder = if (sslTrustAll) {
      val sslBuilder = new SSLContextBuilder()
        .loadTrustMaterial(null, new TrustSelfSignedStrategy())
      val sslsf = new SSLConnectionSocketFactory(
        sslBuilder.build(),
        SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER
      )
      HttpClients.custom().setSSLSocketFactory(sslsf)
    } else {
      HttpClientBuilder.create()
    }
    val config = RequestConfig
      .custom()
      .setConnectTimeout(timeoutInSeconds * 1000)
      .setConnectionRequestTimeout(timeoutInSeconds * 1000)
      .setSocketTimeout(timeoutInSeconds * 1000)
      .build()
    val client = clientBuilder
      .disableAutomaticRetries()
      .setDefaultRequestConfig(config)
      .build()
    client
  }

  def getHttpResponseContent(res: CloseableHttpResponse): String = {
    val entity = res.getEntity()
    if (entity == null) {
      ""
    } else {
      val input = entity.getContent()
      try {
        IOUtils.toString(input, UTF_8)
      } finally {
        input.close()
      }
    }
  }
}
