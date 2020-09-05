package com.exam.decisionmapper

import argonaut.{DecodeJson, Parse}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

import scala.io.Source

object ColumnTransformation {

  def fromFile(
      inputPath: String
  ): Either[JsonDecodeError, List[ColumnTransformation]] = {
    val file = Source.fromFile(inputPath)
    val json =
      try {
        file.mkString
      } finally { file.close() }

    Parse
      .decodeEither[List[ColumnTransformation]](json)
      .left
      .map(JsonDecodeError)
  }

  private implicit def ColumnTransformationDecoder
      : DecodeJson[ColumnTransformation] =
    DecodeJson(c =>
      for {
        existingColumnName <- (c --\ "existing_col_name").as[String]
        newColumnName <- (c --\ "new_col_name").as[String]
        newDataType <- (c --\ "new_data_type").as[String]
        dateExpressions <- (c --\ "date_expression").as[Option[String]]
      } yield ColumnTransformation(
        existingColumnName,
        newColumnName,
        newDataType,
        dateExpressions
      )
    )
}

case class ColumnTransformation(
    existingColumnName: String,
    newColumnName: String,
    newDataType: String,
    dateExpression: Option[String] = None
) {

  def prettyString: String = {
    dateExpression match {
      case Some(e) =>
        s"$existingColumnName -> ($newColumnName, $newDataType, $e)"
      case None => s"$existingColumnName -> ($newColumnName, $newDataType)"
    }

  }
}

case class SupportedColumnTransformation(selectExpr: Column)
object SupportedColumnTransformation {

  private val transformationRules
      : PartialFunction[ColumnTransformation, Column] = {
    case ColumnTransformation(from, to, "date", Some(format)) =>
      to_date(col(from), format).as(to)
    case ColumnTransformation(from, to, "date", None) =>
      to_date(col(from)).as(to)
    case ColumnTransformation(from, to, "boolean", _) =>
      col(from).cast(BooleanType).as(to)
    case ColumnTransformation(from, to, "integer", _) =>
      col(from).cast(IntegerType).as(to)
    case ColumnTransformation(from, to, "string", _) =>
      col(from).as(to)
  }

  def from(
      rawConvertation: ColumnTransformation
  ): Either[UnsupportedTransformationError, SupportedColumnTransformation] = {
    if (transformationRules.isDefinedAt(rawConvertation))
      Right(SupportedColumnTransformation(transformationRules(rawConvertation)))
    else Left(UnsupportedTransformationError(rawConvertation))
  }

  def validateConvertations(
      convertations: List[ColumnTransformation]
  ): Either[MultipleUnsupportedConvertationError, List[
    SupportedColumnTransformation
  ]] = {
    val rawConvertations = convertations.map(SupportedColumnTransformation.from)

    val (errors, supportedConvertations) = rawConvertations.partition(_.isLeft)
    if (errors.isEmpty) Right(supportedConvertations.map(_.right.get))
    else Left(MultipleUnsupportedConvertationError(errors.map(_.left.get)))
  }
}

sealed trait TransformationError {
  val message: String
}

case class JsonDecodeError(message: String) extends TransformationError
case class UnsupportedTransformationError(
    unsupportedTransformation: ColumnTransformation
) extends TransformationError {
  val message =
    s"Unsupported transformation: ${unsupportedTransformation.prettyString}"
}

/**
  * Simple wrapper for dirty emulation of Validated like approach from cats and etc.
  */
case class MultipleUnsupportedConvertationError(
    unsupportedConverters: List[UnsupportedTransformationError]
) extends TransformationError {
  val message =
    s"Unsupported transformations: ${unsupportedConverters.map(_.unsupportedTransformation.prettyString).mkString(",")}"
}
