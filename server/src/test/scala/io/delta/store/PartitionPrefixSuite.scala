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

import io.delta.store.helpers._

class PartitionPrefixSuite extends FunSuite {
  private val logger = LoggerFactory.getLogger(classOf[PartitionPrefixSuite])

  test("date=2023-03-15") {
    val partitionColumns = Seq("date", "hour")
    val predicateHints = Seq("date=\"2023-03-15\"")

    val (prefixFrom, prefixTo) =
      PartitionPrefixHelper.getPrefix(
        partitionColumns,
        predicateHints
      )

    assert(
      prefixFrom == "date=2023-03-15/" && prefixTo == "date=2023-03-15/"
    )
  }

  test("date=2023-03-15 and hour=10") {
    val partitionColumns = Seq("date", "hour")
    val predicateHints = Seq("date=\"2023-03-15\"", "hour=\"10\"")

    val (prefixFrom, prefixTo) =
      PartitionPrefixHelper.getPrefix(
        partitionColumns,
        predicateHints
      )

    assert(
      prefixFrom == "date=2023-03-15/hour=10/" && prefixTo == "date=2023-03-15/hour=10/"
    )
  }

  test("date>=2023-03-15") {
    val partitionColumns = Seq("date", "hour")
    val predicateHints = Seq("date>=\"2023-03-15\"")

    val (prefixFrom, prefixTo) =
      PartitionPrefixHelper.getPrefix(
        partitionColumns,
        predicateHints
      )

    assert(prefixFrom == "date=2023-03-15/" && prefixTo == "")
  }

  test("date<2023-03-15") {
    val partitionColumns = Seq("date", "hour")
    val predicateHints = Seq("date<\"2023-03-15\"")

    val (prefixFrom, prefixTo) =
      PartitionPrefixHelper.getPrefix(
        partitionColumns,
        predicateHints
      )

    assert(prefixFrom == "" && prefixTo == "date=2023-03-15/")
  }

  test("date>=2023-03-15 and date<2023-03-18") {
    val partitionColumns = Seq("date", "hour")
    val predicateHints = Seq("date>=\"2023-03-15\"", "date<\"2023-03-18\"")

    val (prefixFrom, prefixTo) =
      PartitionPrefixHelper.getPrefix(
        partitionColumns,
        predicateHints
      )

    assert(prefixFrom == "date=2023-03-15/" && prefixTo == "date=2023-03-18/")
  }

  test("date>=2023-03-15 and hour=10") {
    val partitionColumns = Seq("date", "hour")
    val predicateHints = Seq("date>=\"2023-03-15\"", "hour=\"10\"")

    val (prefixFrom, prefixTo) =
      PartitionPrefixHelper.getPrefix(
        partitionColumns,
        predicateHints
      )

    assert(prefixFrom == "date=2023-03-15/hour=10/" && prefixTo == "")
  }

  test("date<2023-03-15 and hour=10") {
    val partitionColumns = Seq("date", "hour")
    val predicateHints = Seq("date<\"2023-03-15\"", "hour=\"10\"")

    val (prefixFrom, prefixTo) =
      PartitionPrefixHelper.getPrefix(
        partitionColumns,
        predicateHints
      )

    assert(prefixFrom == "" && prefixTo == "date=2023-03-15/hour=10/")
  }

  test("date>=2023-03-15 and date<2023-03-18 and hour=10") {
    val partitionColumns = Seq("date", "hour")
    val predicateHints =
      Seq("date>=\"2023-03-15\"", "date<\"2023-03-18\"", "hour=\"10\"")

    val (prefixFrom, prefixTo) =
      PartitionPrefixHelper.getPrefix(
        partitionColumns,
        predicateHints
      )

    assert(
      prefixFrom == "date=2023-03-15/hour=10/" && prefixTo == "date=2023-03-18/hour=10/"
    )
  }

  test("date=2023-03-15 and hour>=10") {
    val partitionColumns = Seq("date", "hour")
    val predicateHints = Seq("date=\"2023-03-15\"", "hour>=\"10\"")

    val (prefixFrom, prefixTo) =
      PartitionPrefixHelper.getPrefix(
        partitionColumns,
        predicateHints
      )

    assert(
      prefixFrom == "date=2023-03-15/hour=10/" && prefixTo == "date=2023-03-15/"
    )
  }

  test("date=2023-03-15 and hour<10") {
    val partitionColumns = Seq("date", "hour")
    val predicateHints = Seq("date=\"2023-03-15\"", "hour<\"10\"")

    val (prefixFrom, prefixTo) =
      PartitionPrefixHelper.getPrefix(
        partitionColumns,
        predicateHints
      )

    assert(
      prefixFrom == "date=2023-03-15/" && prefixTo == "date=2023-03-15/hour=10/"
    )
  }

  test("date=2023-03-15 and hour>=10 and hour<15") {
    val partitionColumns = Seq("date", "hour")
    val predicateHints =
      Seq("date=\"2023-03-15\"", "hour>=\"10\"", "hour<\"15\"")

    val (prefixFrom, prefixTo) =
      PartitionPrefixHelper.getPrefix(
        partitionColumns,
        predicateHints
      )

    assert(
      prefixFrom == "date=2023-03-15/hour=10/" && prefixTo == "date=2023-03-15/hour=15/"
    )
  }
}
