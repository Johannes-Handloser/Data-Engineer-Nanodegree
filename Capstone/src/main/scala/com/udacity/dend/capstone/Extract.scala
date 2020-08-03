package com.udacity.dend.capstone
import org.apache.spark.sql._
import com.github.saurfang.sas.spark._

object Extract {

  /**
   * Read out dataFrame out of single File or multiple paths (using wildcard)
   * @param inputPath - InputPath
   * @param interestedColumns - list of columns which should be kept in dataframe
   * @param colsToDropDuplicatesAndNulls - list of columns to drop duplicate rows and null values from
   * @param loadDataFrame - Function to load File to DataFrame
   * @param spark - SparkSession
   * @return
   */
  def apply(
           inputPath: String,
           interestedColumns: List[String],
           colsToDropDuplicatesAndNulls: List[String],
           loadDataFrame: (SparkSession, String) => DataFrame = (spark,inputPath) => spark.read.sas(inputPath),
           filteredColumnFrame: (List[String], DataFrame) => DataFrame = (colToKeep, df) => Logic.filteredColumnFrame(colToKeep, df),
           uniquedDataFrame: (List[String], DataFrame) => DataFrame = (colsToDropDuplicatesAndNulls, df) => Logic.uniquedDataFrame(colsToDropDuplicatesAndNulls, df)
           )(implicit spark: SparkSession) : DataFrame = {
    uniquedDataFrame(colsToDropDuplicatesAndNulls, filteredColumnFrame(interestedColumns, loadDataFrame(spark, inputPath)))
  }
}
