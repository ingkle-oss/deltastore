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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control._

import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import io.delta.store.DeltaStoreAddFile
import io.delta.store.engines._
import io.delta.store.helpers._

class RocksDBSuite extends FunSuite {
  private val logger = LoggerFactory.getLogger(classOf[RocksDBSuite])

  private val engine = {
    val engine = new DeltaStoreRocksDBEngine(
      new DeltaEngineConfig(),
      java.util.UUID.randomUUID().toString
    )
    engine.putAction()(
      "date=2023-02-27/hour=23/part-00001",
      DeltaStoreAddFile(path = "00001")
    )
    engine.putAction()(
      "date=2023-03-12/hour=11/part-00002",
      DeltaStoreAddFile(path = "00002")
    )
    engine.putAction()(
      "date=2023-03-15/hour=10/part-00003",
      DeltaStoreAddFile(path = "00003")
    )
    engine.putAction()(
      "date=2023-03-15/hour=12/part-00004",
      DeltaStoreAddFile(path = "00004")
    )
    engine.putAction()(
      "date=2023-03-15/hour=15/part-00005",
      DeltaStoreAddFile(path = "00005")
    )
    engine.putAction()(
      "date=2023-03-15/hour=18/part-00006",
      DeltaStoreAddFile(path = "00006")
    )
    engine.putAction()(
      "date=2023-03-16/hour=07/part-00007",
      DeltaStoreAddFile(path = "00007")
    )
    engine.putAction()(
      "date=2023-03-16/hour=20/part-00008",
      DeltaStoreAddFile(path = "00008")
    )
    engine.putAction()(
      "date=2023-03-20/hour=10/part-00009",
      DeltaStoreAddFile(path = "00009")
    )
    engine.putAction()(
      "date=2023-04-01/hour=18/part-00010",
      DeltaStoreAddFile(path = "00010")
    )
    engine
  }

  test("date=2023-03-15") {
    val values = engine.getActionsRange()(
      "date=2023-03-15/",
      "date=2023-03-15/"
    )

    assert(values.length == 4)
  }

  test("date=2023-03-15 and hour=10") {
    val values = engine.getActionsRange()(
      "date=2023-03-15/hour=10/",
      "date=2023-03-15/hour=10/"
    )

    assert(values.length == 1)
  }

  test("date>=2023-03-15") {
    val values = engine.getActionsRange()("date=2023-03-15/", "")

    assert(values.length == 8)
  }

  test("date<2023-03-15") {
    val values = engine.getActionsRange()(
      "",
      "date=2023-03-15/"
    )

    assert(values.length == 6)
  }

  test("date>=2023-03-15 and date<2023-03-18") {
    val values = engine.getActionsRange()(
      "date=2023-03-15/",
      "date=2023-03-18/"
    )

    assert(values.length == 6)
  }

  test("date>=2023-03-15 and hour=10") {
    val values = engine.getActionsRange()(
      "date=2023-03-15/hour=10/",
      ""
    )

    assert(values.length == 8)
  }

  test("date<2023-03-15 and hour=10") {
    val values = engine.getActionsRange()(
      "",
      "date=2023-03-15/hour=10/"
    )

    assert(values.length == 3)
  }

  test("date>=2023-03-15 and date<2023-03-18 and hour=10") {
    val values = engine.getActionsRange()(
      "date=2023-03-15/hour=10/",
      "date=2023-03-18/hour=10/"
    )

    assert(values.length == 6)
  }

  test("date=2023-03-15 and hour>=10") {
    val values = engine.getActionsRange()(
      "date=2023-03-15/hour=10/",
      "date=2023-03-15/"
    )

    assert(values.length == 4)
  }

  test("date=2023-03-15 and hour<10") {
    val values = engine.getActionsRange()(
      "date=2023-03-15/",
      "date=2023-03-15/hour=10/"
    )

    assert(values.length == 1)
  }

  test("date=2023-03-15 and hour>=10 and hour<15") {
    val values = engine.getActionsRange()(
      "date=2023-03-15/hour=10/",
      "date=2023-03-15/hour=15/"
    )

    assert(values.length == 3)
  }
}
