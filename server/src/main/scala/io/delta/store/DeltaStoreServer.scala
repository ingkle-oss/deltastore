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

import com.linecorp.armeria.server.Server
import net.sourceforge.argparse4j.ArgumentParsers
import org.slf4j.LoggerFactory
import scalapb.json4s.Printer

object DeltaStoreServer {
  private val parser = {
    val parser = ArgumentParsers
      .newFor("Delta Store Server")
      .build()
      .defaultHelp(true)
      .description("Start the Delta Store Server.")
    parser
      .addArgument("-c", "--configfile")
      .required(true)
      .metavar("CONFIGFILE")
      .dest("configfile")
      .help("The server config file")
    parser
  }

  private def updateDefaultJsonPrinterForScalaPbConverterUtil(): Unit = {
    val module = Class
      .forName("com.linecorp.armeria.server.scalapb.ScalaPbConverterUtil$")
      .getDeclaredField("MODULE$")
      .get(null)
    val defaultJsonPrinterField =
      Class
        .forName("com.linecorp.armeria.server.scalapb.ScalaPbConverterUtil$")
        .getDeclaredField("defaultJsonPrinter")
    defaultJsonPrinterField.setAccessible(true)
    defaultJsonPrinterField.set(module, new Printer())
  }

  private def buildAndStartServer(storeConfig: DeltaStoreConfig): Server = {
    val service = new DeltaStoreSharingService(storeConfig)

    val server = {
      updateDefaultJsonPrinterForScalaPbConverterUtil()

      val builder = Server
        .builder()
        .defaultHostname(storeConfig.host)
        .disableDateHeader()
        .disableServerHeader()
        .requestTimeout(
          java.time.Duration.ofSeconds(storeConfig.requestTimeoutSeconds)
        )
        .annotatedService(storeConfig.endpoint, service: Any)
      builder.http(storeConfig.port)
      builder.build()
    }
    server.start().get()
    server
  }

  def main(args: Array[String]): Unit = {
    val ns = parser.parseArgsOrFail(args)
    val configFile = ns.getString("configfile")
    val storeConfig = DeltaStoreConfig
      .load(configFile)
    DeltaStoreConfigManager.use(storeConfig)
    DeltaStoreTableManager.use(storeConfig)
    DeltaStoreAuthZManager.use(storeConfig)
    val manager = DeltaStoreTableManager()
    manager.ready()
    buildAndStartServer(storeConfig).blockUntilShutdown()
  }
}
