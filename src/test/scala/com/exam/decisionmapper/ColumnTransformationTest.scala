package com.exam.decisionmapper

import java.sql.Date

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{
  BooleanType,
  DateType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ColumnTransformationTest extends AnyFunSuite with Matchers {

  lazy val spark: SparkSession = {
    val s = SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()

    s.sparkContext.setLogLevel("ERROR")

    s
  }

  import spark.implicits._

  test("string->string transformation with changed column name") {
    val transformation = sct("name", "last_name", "string")
    val df = List("John").toDF("name")

    val results = transformDf(df, transformation)

    results.schema should be(
      StructType(Seq(StructField("last_name", StringType)))
    )
    results.collect() should contain theSameElementsAs List(Row("John"))
  }

  test("string->integer transformation with changed column name") {
    val transformation = sct("credit", "debit", "integer")
    val df = List("1").toDF("credit")

    val results = transformDf(df, transformation)

    results.schema should be(StructType(Seq(StructField("debit", IntegerType))))
    results.collect() should contain theSameElementsAs List(Row(1))
  }

  test("string->boolean transformation with changed column name") {
    val transformation = sct("flag", "new_flag", "boolean")
    val df = List("true", "1", "false", "0").toDF("flag")

    val results = transformDf(df, transformation)

    results.schema should be(
      StructType(Seq(StructField("new_flag", BooleanType)))
    )
    results.collect() should contain theSameElementsAs List(
      Row(true),
      Row(true),
      Row(false),
      Row(false)
    )
  }

  test("string->date transformation with default date format yyyy-MM-dd") {
    val transformation = sct("date", "new_date", "date")
    val df = List("2020-01-01").toDF("date")

    val results = transformDf(df, transformation)

    results.schema should be(StructType(Seq(StructField("new_date", DateType))))
    results.collect() should contain theSameElementsAs List(
      Row(Date.valueOf("2020-01-01"))
    )
  }

  test("string->date transformation with custom date format dd-MM-yyyy") {
    val transformation = sct("date", "new_date", "date", Some("dd-MM-yyyy"))
    val df = List("01-02-2020").toDF("date")

    val results = transformDf(df, transformation)

    results.schema should be(StructType(Seq(StructField("new_date", DateType))))
    results.collect() should contain theSameElementsAs List(
      Row(Date.valueOf("2020-02-01"))
    )
  }

  test("unsupported transformation") {
    val transformation = SupportedColumnTransformation.from(
      ColumnTransformation("dec", "new_dec", "decimal")
    )
    transformation.isLeft should be(true)
    transformation.left.get shouldBe a[UnsupportedTransformationError]
  }

  private def transformDf(
      df: DataFrame,
      transformColumn: SupportedColumnTransformation
  ): DataFrame = {
    df.select(transformColumn.selectExpr)
  }

  private def sct(
      from: String,
      to: String,
      dataType: String,
      dateExpression: Option[String] = None
  ) = {
    SupportedColumnTransformation
      .from(ColumnTransformation(from, to, dataType, dateExpression))
      .right
      .get
  }

}
