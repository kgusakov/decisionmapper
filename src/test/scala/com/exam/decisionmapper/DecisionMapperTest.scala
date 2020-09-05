package com.exam.decisionmapper

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{Row, SparkSession}
import com.exam.decisionmapper.records.ProfileRecord
import org.apache.spark.sql.types.{
  DateType,
  IntegerType,
  StringType,
  StructField
}
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

class DecisionMapperTest
    extends AnyFeatureSpec
    with GivenWhenThen
    with Matchers {

  lazy val spark: SparkSession = {
    val s = SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()

    s.sparkContext.setLogLevel("ERROR")

    s
  }

  val decisionMapper = new DecisionMapper(spark)

  Feature("Decision Mapper pipeline") {

    Scenario("Step by step pipeline execution") {

      Given(
        "2 cli args: path to local file with csv data and path to json transformations"
      )
      val (inputDataPath, transformationsPath) = (
        this.getClass.getResource("/sample.csv").getPath,
        this.getClass.getResource("/transformations.json").getPath
      )

      Then("Step 1: read csv data from input file")
      val csvDf = decisionMapper.readCsv(inputDataPath)

      val expectedRows = List(
        Row("John", "26", "26-01-1995", "male"),
        Row("Lisa", "xyz", "26-01-1996", "female"),
        Row(null, "26", "26-01-1995", "male"),
        Row("Julia", " ", "26-01-1995", "female"),
        Row(" ", null, "26-01-1995", null),
        Row("Pete", " ", "26-01-1995", null)
      )
      csvDf.collect() should contain theSameElementsAs (expectedRows)

      Then("Step 2: remove empty strings, but not null string")
      val trimmedCsv = decisionMapper.trimEmptyString(csvDf)
      trimmedCsv.collect() should contain theSameElementsAs (List(
        Row("John", "26", "26-01-1995", "male"),
        Row("Lisa", "xyz", "26-01-1996", "female"),
        Row(null, "26", "26-01-1995", "male")
      ))

      Then("Step 3: Apply transformations from transformations json file")
      val transformedDf =
        decisionMapper.convertColumns(trimmedCsv, transformationsPath)
      transformedDf
        .map(_.schema.toList)
        .right
        .get
        .map(s =>
          (s.name, s.dataType, s.nullable)
        ) should contain theSameElementsAs (List(
        ("first_name", StringType, true),
        ("total_years", IntegerType, true),
        ("d_o_b", DateType, true)
      ))

      Then("Step 4: Receive profile information for every column")
      val profileRecords =
        decisionMapper.calculateProfiling(transformedDf.right.get)
      profileRecords should contain theSameElementsAs List(
        ProfileRecord("first_name", 2, List("John" -> 1, "Lisa" -> 1)),
        ProfileRecord("total_years", 1, List("26" -> 2)),
        ProfileRecord("d_o_b", 2, List("1995-01-26" -> 2, "1996-01-26" -> 1))
      )
    }

    Scenario("Execute the whole pipeline by facade method") {
      import argonaut._
      import Argonaut._

      Given(
        "2 cli args: path to local file with csv data and path to json transformations"
      )
      val (inputDataPath, transformationsPath) = (
        this.getClass.getResource("/sample.csv").getPath,
        this.getClass.getResource("/transformations.json").getPath
      )

      When("Execute the whole pipeline")
      val results = decisionMapper.execute(inputDataPath, transformationsPath)

      Then("Json results must contain appropriate values")

      val expectedJson = List(
        ProfileRecord("first_name", 2, List("John" -> 1, "Lisa" -> 1)),
        ProfileRecord("total_years", 1, List("26" -> 2)),
        ProfileRecord("d_o_b", 2, List("1995-01-26" -> 2, "1996-01-26" -> 1))
      ).asJson

      results should be(Right(expectedJson))
    }
  }

}
