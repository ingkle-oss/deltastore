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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.{
  caseInsensitiveResolution,
  UnresolvedAttribute
}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{
  DateType,
  StringType,
  StructField,
  StructType
}
import org.slf4j.LoggerFactory

import io.delta.store.DeltaStoreAddFile

object PartitionFilterHelper {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val sqlParser = new SparkSqlParser(new SQLConf)

  def evaluatePredicate(
      partitionSchema: StructType,
      allFilters: Seq[String],
      addFiles: Seq[DeltaStoreAddFile]
  ): Seq[DeltaStoreAddFile] = {
    try {
      val addSchema = Encoders.product[DeltaStoreAddFile].schema
      val attributes =
        addSchema.map(f =>
          AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
        )
      val exprs =
        rewritePartitionFilters(
          allFilters
            .flatMap { f =>
              Try(sqlParser.parseExpression(f)).toOption
            }
            .filter(f => isSupportedExpression(f, partitionSchema)),
          partitionSchema,
          attributes
        )
      logger.info(s"Partition filter is ${exprs}")
      if (exprs.isEmpty) {
        addFiles
      } else {
        val converter =
          CatalystTypeConverters.createToCatalystConverter(addSchema)
        val predicate =
          InterpretedPredicate.create(exprs.reduce(And), attributes)
        predicate.initialize(0)
        addFiles.filter { addFile =>
          val res = predicate.eval(converter(addFile).asInstanceOf[InternalRow])
          logger.info(s"Evaluate ${addFile.partitionValues} = ${res}")
          res
        }
      }
    } catch {
      case NonFatal(e) =>
        logger.error(e.getMessage, e)
        addFiles
    }
  }

  private def isSupportedExpression(
      e: Expression,
      schema: StructType
  ): Boolean = {
    def isColumnOrConstant(e: Expression): Boolean = {
      e match {
        case _: Literal => true
        case u: UnresolvedAttribute if u.nameParts.size == 1 =>
          val unquoted = u.name.stripPrefix("`").stripSuffix("`")
          schema.exists(part => caseInsensitiveResolution(unquoted, part.name))
        case c: Cast => isColumnOrConstant(c.child)
        case _       => false
      }
    }

    e match {
      case EqualTo(left, right)
          if isColumnOrConstant(left) && isColumnOrConstant(
            right
          ) =>
        true
      case GreaterThan(left, right)
          if isColumnOrConstant(left) && isColumnOrConstant(
            right
          ) =>
        true
      case LessThan(left, right)
          if isColumnOrConstant(left) && isColumnOrConstant(
            right
          ) =>
        true
      case GreaterThanOrEqual(left, right)
          if isColumnOrConstant(left) && isColumnOrConstant(
            right
          ) =>
        true
      case LessThanOrEqual(left, right)
          if isColumnOrConstant(left) && isColumnOrConstant(
            right
          ) =>
        true
      case EqualNullSafe(left, right)
          if isColumnOrConstant(left) && isColumnOrConstant(
            right
          ) =>
        true
      case IsNull(e) if isColumnOrConstant(e) =>
        true
      case IsNotNull(e) if isColumnOrConstant(e) =>
        true
      case Not(e) if isSupportedExpression(e, schema) =>
        true
      case Or(left, right)
          if isSupportedExpression(left, schema) &&
            isSupportedExpression(right, schema) =>
        true
      case And(left, right)
          if isSupportedExpression(left, schema) &&
            isSupportedExpression(right, schema) =>
        true
      case _ => false
    }
  }

  private def rewritePartitionFilters(
      partitionFilters: Seq[Expression],
      partitionSchema: StructType,
      attributes: Seq[Attribute]
  ): Seq[Expression] = {
    val attribute = attributes.find(_.name == "partitionValues").head
    partitionFilters.map(_.transformUp { case a: Attribute =>
      val unquoted = a.name.stripPrefix("`").stripSuffix("`")
      val column = partitionSchema.find { field =>
        field.name == unquoted
      }
      column match {
        case Some(StructField(name, dataType, _, _)) =>
          dataType match {
            case DateType =>
              ExtractValue(
                attribute,
                Literal(name),
                org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
              )
            case _ =>
              Cast(
                ExtractValue(
                  attribute,
                  Literal(name),
                  org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
                ),
                dataType
              )
          }
        case None =>
          UnresolvedAttribute(Seq("partitionValues", a.name))
      }
    })
  }
}
