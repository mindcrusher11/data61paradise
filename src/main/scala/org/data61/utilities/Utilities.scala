package org.data61.utilities

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.data61.traitimpl.MySqlDataReader
import org.data61.utilities.Utilities.{addLabelToNodes, removeAddressColumn}
import org.apache.spark.sql.functions.udf

import scala.reflect.internal.util.TableDef.Column

object Utilities {

  val confValue = ConfigFactory.load()

  def getConfiguration(): Config ={
    val confValues = ConfigFactory.load()
    confValues
  }

  def edgeColNames(df:DataFrame):DataFrame ={
     df.toDF(":START_ID",":TYPE",":END_ID")
  }

  def addIdToNodes(df:DataFrame) : DataFrame ={
    df.withColumnRenamed(Constants.nodeCurrColName, Constants.nodeNewColName)
  }

  def addLabelToNodes(frame: DataFrame) :DataFrame ={
    frame.toDF(":LABEL","valid_until","country_codes","countries","node_id:ID","sourceID",
      "address","name","jurisdiction_description","service_provider","jurisdiction","closed_date",
      "incorporation_date","ibcRUC","type","status","company_type","note"

    )
  }

  import org.apache.spark.sql.functions.udf
  val removeBrackets = udf[Option[String], String](cleanLabels1)

  val removeQuoteUdf = udf[Option[String], String](removeQuotes)

  def cleanLabels1(inputString:String): Option[String] ={
    val str = Option(inputString).getOrElse(return None)
    Some(str.replace("[\"", "").replace("\"]",""))
  }

  def cleanKeywords(df:DataFrame):DataFrame ={
    val processedDf = df.columns.foldLeft(df){(memoDF, colName) =>
      memoDF.withColumn(
        colName,
        removeQuoteUdf(memoDF(colName))
      )

    }
    processedDf
  }

  def removeQuotes(inputString:String):Option[String] = {
    val str = Option(inputString).getOrElse(return None)
    Some(str.replace("\"",""))
  }

  def cleanLabels(inputString:String): String ={
    inputString.replace("[]".toCharArray, "")
  }

  def removeAddressColumn(df:DataFrame,colName:String = "address"):DataFrame ={
    df.drop(colName)
  }

  def main(args:Array[String]): Unit ={

    val edgesTblDf = MySqlDataReader.getJdbcDFR().option("dbtable", "paradise.edges").load()
    val entityTblDf = MySqlDataReader.getJdbcDFR().option("dbtable", "paradise.`nodes.entity`").load()
    val officerTblDf = MySqlDataReader.getJdbcDFR().option("dbtable", "paradise.`nodes.officer`").load()
    val intermediaryTblDf = MySqlDataReader.getJdbcDFR().option("dbtable", "paradise.`nodes.intermediary`").load()
    val othersTblDf = MySqlDataReader.getJdbcDFR().option("dbtable", "paradise.`nodes.other`").load()
    val addressTblDf = MySqlDataReader.getJdbcDFR().option("dbtable", "paradise.`nodes.address`").load()

    val edgesDf = edgeColNames(edgesTblDf.select("node_1" , "rel_type","node_2" ))
    val entityDf = addLabelToNodes(addIdToNodes(entityTblDf))
    val officerDf = addLabelToNodes(addIdToNodes(officerTblDf))
    val intermediaryDf = addLabelToNodes(addIdToNodes(intermediaryTblDf))
    val othersDf = addLabelToNodes(addIdToNodes(othersTblDf))
    val addressDf = addLabelToNodes(addIdToNodes(addressTblDf))

    edgesDf.printSchema()
    entityDf.printSchema
    officerDf.printSchema
    intermediaryDf.printSchema
    othersDf.printSchema
    addressDf.printSchema

    edgesDf.show(5)
    entityDf.show(5)
    officerDf.show(5)
    intermediaryDf.show(5)
    othersDf.show(5)
    addressDf.show(5)

    val entityDFNoBrackets = entityDf.withColumn(Constants.nodeLblNewColName,removeBrackets(entityDf(Constants.nodeLblNewColName)))
    val officerDFNoBrackets =  officerDf.withColumn(Constants.nodeLblNewColName,removeBrackets(officerDf(Constants.nodeLblNewColName)))
    val intermediaryDFNoBrackets= intermediaryDf.withColumn(Constants.nodeLblNewColName,removeBrackets(intermediaryDf(Constants.nodeLblNewColName)))
    val othersDFNoBrackets = othersDf.withColumn(Constants.nodeLblNewColName,removeBrackets(othersDf(Constants.nodeLblNewColName)))
    val addressDFNoBrackets = addressDf.withColumn(Constants.nodeLblNewColName,removeBrackets(addressDf(Constants.nodeLblNewColName)))

    val entityNoAddress = removeAddressColumn(entityDFNoBrackets)
    val officerNoAddress = removeAddressColumn(officerDFNoBrackets)
    val intermediaryNoAddress =  removeAddressColumn(intermediaryDFNoBrackets)
    val othersNoAddress = removeAddressColumn(othersDFNoBrackets)

    val entityNoQuotes = cleanKeywords(entityNoAddress)
    val officerNoQuotes = cleanKeywords(officerNoAddress)
    val intermediaryNoQuotes =  cleanKeywords(intermediaryNoAddress)
    val othersNoQuotes = cleanKeywords(othersNoAddress)
    val addressNoQuotes = cleanKeywords(addressDFNoBrackets)

    edgesDf.coalesce(1).write.mode(SaveMode.Overwrite).option("header","true").csv("edges.csv")
    entityNoQuotes.write.mode(SaveMode.Overwrite).option("header","true").csv("entity.csv")
    officerNoQuotes.write.mode(SaveMode.Overwrite).option("header","true").csv("officer.csv")
    intermediaryNoQuotes.write.mode(SaveMode.Overwrite).option("header","true").csv("intermediary.csv")
    othersNoQuotes.write.mode(SaveMode.Overwrite).option("header","true").csv("others.csv")
    addressNoQuotes.write.mode(SaveMode.Overwrite).option("header","true").csv("address.csv")

    MySqlDataReader.stopSparkSession()

  }
}
