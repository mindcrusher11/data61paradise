package org.data61.traitimpl

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.data61.utilities.Utilities

object MySqlDataReader {

  val sparkSession = SparkConfInfo.getSparkSession

  def getJdbcDF():DataFrame ={

    val jdbcDF = sparkSession.read
      .format(Utilities.confValue.getString("data61.mysql.format"))
      .option("url", Utilities.confValue.getString("data61.mysql.jdbcurl"))
      .option("dbtable", "information_schema.tables")
      .option("user", Utilities.confValue.getString("data61.mysql.username"))
      .option("password", Utilities.confValue.getString("data61.mysql.password"))
      .load()
    jdbcDF
  }

  def getJdbcDFR():DataFrameReader ={
    val jdbcDF = sparkSession.read
      .format(Utilities.confValue.getString("data61.mysql.format"))
      .option("url", Utilities.confValue.getString("data61.mysql.jdbcurl"))

      .option("user", Utilities.confValue.getString("data61.mysql.username"))
      .option("password", Utilities.confValue.getString("data61.mysql.password"))
        .option("driver","com.mysql.jdbc.Driver")
      //.load()
    jdbcDF
  }

  def stopSparkSession(): Unit ={
    sparkSession.stop()
  }

}
