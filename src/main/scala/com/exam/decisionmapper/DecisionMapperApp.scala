package com.exam.decisionmapper

import org.apache.spark.sql.SparkSession

object DecisionMapperApp {
  def main(args: Array[String]) {

    if (args.length != 2) {
      println(
        "Please provide the paths to input data file and to JSON file with transformations"
      )

      System.exit(1)
    }

    val (inputDataPath, transformationsPath) = (args(0), args(1))

    val spark = SparkSession.builder.appName("Decision Mapper").getOrCreate()
    val decisionMapper = new DecisionMapper(spark)
    decisionMapper.execute(inputDataPath, transformationsPath) match {
      case Left(err)    => throw new RuntimeException(err.message)
      case Right(value) => println(value.spaces2)
    }
    spark.stop()
  }
}
