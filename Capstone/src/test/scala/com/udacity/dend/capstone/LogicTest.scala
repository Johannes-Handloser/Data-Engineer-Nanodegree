package com.udacity.dend.capstone

import com.udacity.dend.capstone.utils.SparkSpec
import org.scalatest.{FlatSpec, Matchers}

class LogicTest extends FlatSpec with SparkSpec with Matchers {

  behavior of "Logic"

  it should "get a full list of .csv files in test resource dir" in {
    val resourcePath = getClass.getResource("/").getPath
    Logic.listOfFilesInDir(resourcePath, ".csv").size should be (2)
  }

  it should "drop all columns on df expect the list of colToKeep" in {
    val immigrationPath = getClass.getResource("/immigration_data_sample.csv").getPath
    val colsToKeep = List("i94yr", "i94mon", "i94cit", "i94port", "arrdate", "i94mode", "depdate", "i94bir", "i94visa")
    Logic.filteredColumnFrame(colsToKeep,
      spark.read.format("csv").option("header", "true").load(immigrationPath)
    ).columns.toList should be (colsToKeep)
  }

  it should "keep columns and clean null values" in {
    val inputPath = getClass.getResource("/TempByCitySnippet.csv").getPath
    val list = List("dt", "AverageTemperature", "AverageTemperatureUncertainty", "City", "Country", "Latitude", "Longitude")
    val df = Logic.uniquedDataFrame(list, spark.read.format("csv").option("header", "true").load(inputPath))
    df.columns.length should be (7)
    df.count() should be (10)
  }

}
