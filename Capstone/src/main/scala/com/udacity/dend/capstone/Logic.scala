package com.udacity.dend.capstone

import java.io.File

import org.apache.spark.sql.{Column, DataFrame}

object Logic {

  val listOfFilesInDir: (String, String) => List[String] = (dir, fileSuffix) => {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(fileSuffix))
      .map(_.getPath).toList
  }

  val filteredColumnFrame: (List[String], DataFrame) => DataFrame = (colToKeep, df) =>
    df.select(df.columns.filter(colName => colToKeep.contains(colName)).map(colName => new Column(colName)): _*)

  val uniquedDataFrame: (List[String], DataFrame) => DataFrame = (colsToDropDuplicatesAndNulls, df) => {
    df.dropDuplicates(colsToDropDuplicatesAndNulls).na.drop(colsToDropDuplicatesAndNulls)
  }

}
