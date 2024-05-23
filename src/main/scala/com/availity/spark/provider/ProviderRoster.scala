package com.availity.spark.provider


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.scala.Logging
import com.availity.spark.utils.Utils
import com.availity.spark.utils.Constants
import com.availity.spark.spark.SparkApplication

object ProviderRoster extends SparkApplication with Logging{
 /*Following property was set to run the code in local windows machine,commented to run in cluster*/
  // System.setProperty("hadoop.home.dir","C:\\winutils")
  override def appName: String = Constants.appName
  override def master: String = Constants.clusterMode // change the cluster mode to run on cluster or client , defaulted to local
  override def sparkAppConf: Map[String, String] = Map[String,String](
    "hive.exec.dynamic.partition"->"true",
    "hive.exec.dynamic.partition.mode"->"nonstrict",
    "hive.shuffle.consolidatedFiles"->"true",
    "spark.sql.broadcastTimeout"->"4000",
    "spark.sql.autoBroadcastJoinThreshold"->"40m",
    "spark.sql.shuffle.partition"->"2000")
private var sparkSession= null.asInstanceOf[SparkSession]
  def main(args: Array[String]): Unit = {
    sparkSession=getSparkSession
    try {
      val inputDir = args(0) // provide Input directory (HDFS or s3) for the provider and visits file (place both files in same directory and pass directory path as parameter)
      val outputDir = args(1) // provide output directory for the Json Files (HDFS or s3)
      val providerDir = inputDir + "/providers.csv" // builds provider input file path
      val visitsDir = inputDir + "/visits.csv" // builds visits input file path
      val problemStatementOutputPath_1 = outputDir + "/problemStatment_1" // builds problem statement 1 output file path
      val problemStatementOutputPath_2 = outputDir + "/problemStatment_2" // builds problem statement 2 output file path
      /*
          * START TRANSFORMATION
      */
      logger.info("Reading csv files to create provider and visits dataframe==>")
      val providerDF = Utils.readCsvData(sparkSession,providerDir, "|", "true").withColumn("provider_full_name", concat_ws(" ", col("first_name"), col("last_name"))) // comcatinated first_name and last_name colomns to create provider_full_name
      val visitsDF = Utils.readCsvData(sparkSession,visitsDir, ",", "false").toDF("member_id", "provider_id", "visit_date").withColumn("visit_month", date_format(to_date(col("visit_date"), "yyyy-MM-dd"), "MMMM")) // derive visit_month from date visit date

      logger.info("Performing transformations==>")
      val providerVisitDF = providerDF.join(visitsDF, Seq("provider_id"))

      // The following functionality can also be achieved using window spec and aggregate functions
      val problemStatement1_DF = providerVisitDF.groupBy("provider_id", "provider_full_name", "provider_specialty").count().toDF("provider_id", "provider_full_name", "provider_specialty", "number_of_visits")
      // Grouping the records based on member ID,full name,provider_speciality and counting the total number of months
      val problemStatement2_DF = providerVisitDF.groupBy("provider_id", "visit_month").count().toDF("provider_id", "visit_month", "number_of_visits").orderBy(col("provider_id"), unix_timestamp(col("visit_month"), "MMMM"))
      // Grouping the records based on member ID,full name,provider_speciality and counting the total number of months

      logger.info("Writing Json files for problem statement 1 and 2==>")
      Utils.writeJsonFileWithPattition(problemStatement1_DF, problemStatementOutputPath_1, "provider_specialty") // this will create partition folder inside the specified folder and associated Json file as part-file inside the partition folder.
      Utils.writeJsonFile(problemStatement2_DF, problemStatementOutputPath_2) // this will create part-files inside the specified folder which will have single Json file for all members
    } catch {
      case e: Exception =>
        logger.error("Problem statement could not be solved", e);
    }
  }


}
