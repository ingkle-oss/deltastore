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

import scala.util.Try
import scala.util.control.Breaks
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.slf4j.LoggerFactory

object PartitionPrefixHelper {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val sqlParser = new SparkSqlParser(new SQLConf)

  def getPrefix(
      partitionColumns: Seq[String],
      partitionFilters: Seq[String]
  ): (String, String) = {
    var prefixFrom = ""
    var prefixTo = ""
    var donePrefixFrom = false
    var donePrefixTo = false
    val partitionExpressions = partitionFilters
      .flatMap { f => Try(sqlParser.parseExpression(f)).toOption }
    val loop = new Breaks
    loop.breakable {
      for (c <- partitionColumns) {
        var hasPartition = false
        var hasPrefixFrom = false
        var hasPrefixTo = false
        for (e <- partitionExpressions) {
          e match {
            case q: EqualTo =>
              val name = q.left.toString.stripPrefix("'")
              if (c == name) {
                if (!donePrefixFrom) { prefixFrom += s"${name}=${q.right}/" }
                if (!donePrefixTo) { prefixTo += s"${name}=${q.right}/" }
                hasPartition = true
                hasPrefixFrom = true
                hasPrefixTo = true
              }
            case q: GreaterThan =>
              val name = q.left.toString.stripPrefix("'")
              if (c == name) {
                if (!donePrefixFrom) { prefixFrom += s"${name}=${q.right}/" }
                hasPartition = true
                hasPrefixFrom = true
              }
            case q: GreaterThanOrEqual =>
              val name = q.left.toString.stripPrefix("'")
              if (c == name) {
                if (!donePrefixFrom) { prefixFrom += s"${name}=${q.right}/" }
                hasPartition = true
                hasPrefixFrom = true
              }
            case q: LessThan =>
              val name = q.left.toString.stripPrefix("'")
              if (c == name) {
                if (!donePrefixTo) { prefixTo += s"${name}=${q.right}/" }
                hasPartition = true
                hasPrefixTo = true
              }
            case q: LessThanOrEqual =>
              val name = q.left.toString.stripPrefix("'")
              if (c == name) {
                if (!donePrefixTo) { prefixTo += s"${name}=${q.right}/" }
                hasPartition = true
                hasPrefixTo = true
              }
            case _ => // do nothing
          }
        }
        if (!hasPrefixFrom) { donePrefixFrom = true }
        if (!hasPrefixTo) { donePrefixTo = true }
        if (!hasPartition) loop.break
      }
    }
    (prefixFrom, prefixTo)
  }
}
