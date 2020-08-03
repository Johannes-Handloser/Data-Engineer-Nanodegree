package com.udacity.dend.capstone

import com.udacity.dend.capstone.utils.SparkSpec
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}


class ExtractTest extends FlatSpec with SparkSpec with Matchers {

  behavior of "Extract"

  it should "give the right columns from temperature dataset from csv snippet" in {
    val inputPath = getClass.getResource("/TempByCitySnippet.csv").getPath
    val loadDataFrame: (SparkSession, String) => DataFrame = (spark, inputPath) => spark.read.format("csv").option("header", "true").load(inputPath)
    val expectedColList = List("dt", "AverageTemperature", "AverageTemperatureUncertainty", "City", "Country", "Latitude", "Longitude")
    Extract.apply(inputPath, expectedColList, List.empty, loadDataFrame)(spark).columns.toList should be (expectedColList)
  }

  it should "give the right columns from the immigration dataset from csv snippet" in {
    val inputPath = getClass.getResource("/immigration_data_sample.csv").getPath
    val loadDataFrame: (SparkSession, String) => DataFrame = (spark, inputPath) => spark.read.format("csv").option("header", "true").load(inputPath)
    val expectedColList = List("id", "cicid", "i94yr", "i94mon", "i94cit", "i94res", "i94port", "arrdate",
    "i94mode", "i94addr", "depdate", "i94bir", "i94visa", "count",
    "dtadfile", "visapost", "occup", "entdepa", "entdepd", "entdepu",
    "matflag", "biryear", "dtaddto", "gender", "insnum", "airline",
    "admnum", "fltno", "visatype")
    Extract.apply(inputPath, expectedColList, List.empty, loadDataFrame)(spark).columns.toList should be (expectedColList)
  }

  it should "clean df the right way and only persist interestedCol List" in {
    // TempByCitySnippet has 10 entries without null in AverageTemperature and duplicate rows
    val expectedCount = 10
    val inputPath = getClass.getResource("/TempByCitySnippet.csv").getPath
    val loadDataFrame: (SparkSession, String) => DataFrame = (spark, inputPath) => spark.read.format("csv").option("header", "true").load(inputPath)
    val colsToDropDuplicatesAndNulls = List("AverageTemperature")
    val interestedColumns = List("AverageTemperature", "AverageTemperatureUncertainty", "City", "Country")
    val df = Extract(inputPath, interestedColumns, colsToDropDuplicatesAndNulls, loadDataFrame)(spark)
    df.count() should be (expectedCount)
    df.columns.toList should be (List("AverageTemperature", "AverageTemperatureUncertainty", "City", "Country"))

  }
}
