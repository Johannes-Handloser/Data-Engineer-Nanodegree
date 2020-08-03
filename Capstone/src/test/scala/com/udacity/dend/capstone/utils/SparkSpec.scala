package com.udacity.dend.capstone.utils

import org.apache.spark.sql.SparkSession

trait SparkSpec {

  lazy val spark: SparkSession = {
    SparkSession
      .builder
      .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")
      .master("local")
      .appName("spark test")
      .getOrCreate()
  }
}
