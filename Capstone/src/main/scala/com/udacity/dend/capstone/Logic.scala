package com.udacity.dend.capstone

import java.io.File

import scala.io.Source
import org.apache.spark.sql.{Column, DataFrame}

object Logic {

  /**
   * returns a list of files inside a directory matching a provided file suffix
   */
  val listOfFilesInDir: (String, String) => List[String] = (dir, fileSuffix) => {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(fileSuffix))
      .map(_.getPath).toList
  }

  /**
   * drops all columns besides the list of columns to keep
   */
  val filteredColumnFrame: (List[String], DataFrame) => DataFrame = (colToKeep, df) =>
    df.select(df.columns.filter(colName => colToKeep.contains(colName)).map(colName => new Column(colName)): _*)

  /**
   * drop duplicate rows and null-values from all column names provided
   */
  val uniquedDataFrame: (List[String], DataFrame) => DataFrame = (colsToDropDuplicatesAndNulls, df) => {
    df.dropDuplicates(colsToDropDuplicatesAndNulls).na.drop(colsToDropDuplicatesAndNulls)
  }

  /**
   * reads SAS Header file and returns list of city port tuples
   */
  val toCityPortList: String => List[(String,String)] = path => {
    val source = Source.fromFile(path)
    val sasList = source.getLines().toList
    source.close()
    // regex to get 3 digit port out SAS header file
    val portPattern = """([A-Z]{3})""".r
    // regex to get city out SAS header file
    val cityPattern = """(?<==\s')(.*)(?=,)""".r
    // interval where city - port mapping is located in index
    val portCityList = sasList.slice(302, 962)
    val ports = portCityList.map(line => portPattern.findFirstMatchIn(line))
    val cities = portCityList.map(line => cityPattern.findFirstMatchIn(line))
    ports.zip(cities)
      .filter(_._1.isDefined)
      .filter(_._2.isDefined)
      .map(x => (x._1.get.toString(), x._2.get.toString().toLowerCase))
  }

}
