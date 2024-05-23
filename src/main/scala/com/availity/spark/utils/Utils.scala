package com.availity.spark.utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.logging.log4j.scala.Logging

/*Define all Util methods here*/
object Utils extends Logging {

  //method to read csv file,accepts input path,header and visit date as parameter
  def readCsvData(spark:SparkSession,path: String, delimiter: String, header: String): DataFrame = {
    var csvDF: DataFrame = null
    try {
      csvDF = spark.read
        .format("csv")
        .option("header", header)
        .option("delimiter", delimiter)
        .option("treatEmptyValuesAsNulls", "false")
        .load(path)
    } catch {
      case e: Exception => logger.error(s"Could not read the csv file provided=>" + e)
    }
    return csvDF
  }
  //Method to write Json file with partition , accepts dataframe ,output paths and partition col as parameters
  def writeJsonFileWithPattition(df: DataFrame, path: String, partitionCol: String) {
    try {
      df.write.mode("overwrite").format("json").partitionBy(partitionCol).save(path) // save mode can be changed to append or overwrite per use case
    } catch {
      case e: Exception => logger.error(s"Could not write Json File=>" + e)
    }
  }
  //Method to write Json file with partition , accepts dataframe and output paths as parameters
  def writeJsonFile(df: DataFrame, path: String) {
    try {
      df.write.mode("overwrite").format("json").save(path) // save mode can be changed to append or overwrite per use case
    } catch {
      case e: Exception => logger.error(s"Could not write Json File=>" + e)
    }
  }

}
