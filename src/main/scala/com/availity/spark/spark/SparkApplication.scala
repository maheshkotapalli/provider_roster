package com.availity.spark.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
/*this trait is created to initiliaze spark session, and also set spark execution properties*/
trait SparkApplication {
  def sparkAppConf: Map[String, String]
  def appName: String
  def master: String
  private var sparkSession = null.asInstanceOf[SparkSession]

  @throws(classOf[SparkException])
  def getSparkSession: SparkSession = {
    if (sparkSession == null) {
      sparkSession=SparkSession.builder().appName(appName).config("spark.master", "local").getOrCreate()
      sparkAppConf.foreach { case (k, v) => sparkSession.conf.set(k, v)}
    }
    sparkSession
  }
}