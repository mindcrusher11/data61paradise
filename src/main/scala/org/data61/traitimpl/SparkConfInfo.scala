package org.data61.traitimpl

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.Level
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.data61.itraits.ConfInfo
import org.data61.utilities.Utilities
import org.apache.log4j.{Level, Logger}

object SparkConfInfo extends ConfInfo{

  var sc:SparkContext = null
  val propValues = Utilities.getConfiguration()

  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  def getSparkConf():SparkConf = {
    val conf = new SparkConf()
    .setMaster(propValues.getString("data61.spark.masterurl"))
    .setAppName(propValues.getString("data61.spark.appname"))
   conf
  }

  def getSparkContext:SparkContext = {
    if(sc == null) sc = new SparkContext(getSparkConf())
    sc
  }

  def getSQlContext:SQLContext={
    val sQLContext = new SQLContext(getSparkContext)
    sQLContext
  }

  def stopSpark(): Unit ={
    getSparkContext.stop()
  }

  def getSparkSession:SparkSession ={
    val sparkSession = SparkSession
      .builder()
      .master(propValues.getString("data61.spark.masterurl"))
      .appName(propValues.getString("data61.spark.appname"))
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("OFF")
    sparkSession
  }

  def stopSparkSession(): Unit ={
    getSparkContext.stop()
  }
}
