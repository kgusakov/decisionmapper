package com.exam.decisionmapper

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, trim}
import argonaut.{EncodeJson, Json}
import records._

class DecisionMapper(val sparkSession: SparkSession) {

  import sparkSession.implicits._

  def readCsv(path: String): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("header", true)
      .load(path)
  }

  def trimEmptyString(df: DataFrame): DataFrame = {
    df.columns
      .foldLeft(df)((df, c) =>
        df.filter(col(c).isNull || trim(col(c)).notEqual(""))
      )
  }

  def convertColumns(
      df: DataFrame,
      transformationsPath: String
  ): Either[TransformationError, DataFrame] = {
    for {
      convertations <- ColumnTransformation.fromFile(transformationsPath)
      supportedConvertations <-
        SupportedColumnTransformation.validateConvertations(convertations)
    } yield {
      df.select(supportedConvertations.map(_.selectExpr): _*)
    }
  }

  def calculateProfiling(df: DataFrame): Seq[ProfileRecord] = {
    for (c <- df.columns) yield {
      val profilingData =
        df
          .filter(col(c).isNotNull)
          .groupBy(col(c))
          .count()
          .map(r => r.get(0).toString -> r.getLong(1))
          .collect()

      ProfileRecord(c, profilingData.length, profilingData.toList)
    }
  }

  def execute(
      inputDataPath: String,
      transformationsPath: String
  ): Either[TransformationError, Json] = {
    for (
      transformedDf <- convertColumns(
        trimEmptyString(readCsv(inputDataPath)),
        transformationsPath
      )
    ) yield {
      val results = calculateProfiling(transformedDf)
      EncodeJson.of[List[ProfileRecord]].encode(results.toList)
    }
  }
}
