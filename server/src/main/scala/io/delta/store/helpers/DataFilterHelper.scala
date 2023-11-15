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

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.{
  caseInsensitiveResolution,
  UnresolvedAttribute
}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory
import scala.util.Try
import scala.util.control.Breaks
import scala.util.control.NonFatal

import io.delta.store.DeltaStoreAddFile

case class AddFileStats(
    minValues: Map[String, String],
    maxValues: Map[String, String]
)

object DataFilterHelper {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val sqlParser = new SparkSqlParser(new SQLConf)

  private val NUM_RECORDS = "numRecords"
  private val MIN_VALUES = "minValues"
  private val MAX_VALUES = "maxValues"
  private val NULL_COUNT = "nullCount"

  def evaluatePredicate(
      dataSchema: StructType,
      allFilters: Seq[String],
      addFiles: Seq[DeltaStoreAddFile]
  ): Seq[DeltaStoreAddFile] = {
    try {
      var statsSchema = new StructType()
        .add(MIN_VALUES, MapType(StringType, StringType))
        .add(MAX_VALUES, MapType(StringType, StringType))
      val attributes =
        statsSchema.map(f =>
          AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
        )
      val exprs =
        rewriteDataFilters(
          allFilters
            .flatMap { f =>
              Try(sqlParser.parseExpression(f)).toOption
            }
            .filter(f => isSupportedExpression(f, dataSchema)),
          dataSchema,
          attributes
        )
      logger.info(s"Data filter is ${exprs}")
      if (exprs.isEmpty) {
        addFiles
      } else {
        val converter =
          CatalystTypeConverters.createToCatalystConverter(statsSchema)
        val predicate =
          InterpretedPredicate.create(exprs.reduce(And), attributes)
        predicate.initialize(0)
        addFiles.filter { addFile =>
          val stats = JsonHelper.fromJson[AddFileStats](addFile.stats)
          val res = predicate.eval(converter(stats).asInstanceOf[InternalRow])
          logger.info(s"Evaluate ${stats} = ${res}")
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

  private def rewriteDataFilters(
      dataFilters: Seq[Expression],
      dataSchema: StructType,
      attributes: Seq[Attribute]
  ): Seq[Expression] = {
    val minValues = attributes.find(_.name == MIN_VALUES).head
    val maxValues = attributes.find(_.name == MAX_VALUES).head
    dataFilters.map {
      case e: EqualTo =>
        (e.left, e.right) match {
          case (l: Attribute, r: Literal) =>
            val unquoted = l.name.stripPrefix("`").stripSuffix("`")
            val column = dataSchema.find { field =>
              field.name == unquoted
            }
            column match {
              case Some(StructField(name, dataType, _, _)) =>
                val min = Cast(
                  ExtractValue(
                    minValues,
                    Literal(name),
                    org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
                  ),
                  dataType
                )
                val max = Cast(
                  ExtractValue(
                    maxValues,
                    Literal(name),
                    org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
                  ),
                  dataType
                )
                val v = Cast(r, dataType)
                Or(
                  IsNull(min),
                  And(
                    LessThanOrEqual(min, v),
                    GreaterThanOrEqual(max, v)
                  )
                )
              case _ => Literal(true)
            }
          case _ => Literal(true)
        }
      case e: EqualNullSafe =>
        (e.left, e.right) match {
          case (l: Attribute, r: Literal) =>
            val unquoted = l.name.stripPrefix("`").stripSuffix("`")
            val column = dataSchema.find { field =>
              field.name == unquoted
            }
            column match {
              case Some(StructField(name, dataType, _, _)) =>
                val min = Cast(
                  ExtractValue(
                    minValues,
                    Literal(name),
                    org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
                  ),
                  dataType
                )
                val max = Cast(
                  ExtractValue(
                    maxValues,
                    Literal(name),
                    org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
                  ),
                  dataType
                )
                val v = Cast(r, dataType)
                Or(
                  IsNull(min),
                  Or(
                    GreaterThan(min, v),
                    LessThan(max, v)
                  )
                )
              case _ => Literal(true)
            }
          case _ => Literal(true)
        }
      case e: GreaterThan =>
        (e.left, e.right) match {
          case (l: Attribute, r: Literal) =>
            val unquoted = l.name.stripPrefix("`").stripSuffix("`")
            val column = dataSchema.find { field =>
              field.name == unquoted
            }
            column match {
              case Some(StructField(name, dataType, _, _)) =>
                val max = Cast(
                  ExtractValue(
                    maxValues,
                    Literal(name),
                    org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
                  ),
                  dataType
                )
                val v = Cast(r, dataType)
                Or(
                  IsNull(max),
                  GreaterThan(max, v)
                )
              case _ => Literal(true)
            }
          case _ => Literal(true)
        }
      case e: GreaterThanOrEqual =>
        (e.left, e.right) match {
          case (l: Attribute, r: Literal) =>
            val unquoted = l.name.stripPrefix("`").stripSuffix("`")
            val column = dataSchema.find { field =>
              field.name == unquoted
            }
            column match {
              case Some(StructField(name, dataType, _, _)) =>
                val max = Cast(
                  ExtractValue(
                    maxValues,
                    Literal(name),
                    org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
                  ),
                  dataType
                )
                val v = Cast(r, dataType)
                Or(
                  IsNull(max),
                  GreaterThanOrEqual(max, v)
                )
              case _ => Literal(true)
            }
          case _ => Literal(true)
        }
      case e: LessThan =>
        (e.left, e.right) match {
          case (l: Attribute, r: Literal) =>
            val unquoted = l.name.stripPrefix("`").stripSuffix("`")
            val column = dataSchema.find { field =>
              field.name == unquoted
            }
            column match {
              case Some(StructField(name, dataType, _, _)) =>
                val min = Cast(
                  ExtractValue(
                    minValues,
                    Literal(name),
                    org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
                  ),
                  dataType
                )
                val v = Cast(r, dataType)
                Or(
                  IsNull(min),
                  LessThan(min, v)
                )
              case _ => Literal(true)
            }
          case _ => Literal(true)
        }
      case e: LessThanOrEqual =>
        (e.left, e.right) match {
          case (l: Attribute, r: Literal) =>
            val unquoted = l.name.stripPrefix("`").stripSuffix("`")
            val column = dataSchema.find { field =>
              field.name == unquoted
            }
            column match {
              case Some(StructField(name, dataType, _, _)) =>
                val min = Cast(
                  ExtractValue(
                    minValues,
                    Literal(name),
                    org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
                  ),
                  dataType
                )
                val v = Cast(r, dataType)
                Or(
                  IsNull(min),
                  LessThanOrEqual(min, v)
                )
              case _ => Literal(true)
            }
          case _ => Literal(true)
        }
      case _ => Literal(true)
    }
  }
}
