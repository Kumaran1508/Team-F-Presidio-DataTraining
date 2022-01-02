package com.project.transformation
import com.project.util.UtilityLoader
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class TotalRevenue extends UtilityLoader {

  def queryCreator(): Unit ={


    //Extraction of Revenue of Retailer France
    val totalRevenue = sparkSession.sql("SELECT `Revenue` FROM SALES_TABLE WHERE `Retailer country`='France'")

    calculateRevenue(totalRevenue)
  }

  def calculateRevenue(revenues : DataFrame): Unit ={
    //calculating total revenue of the country France
    val total = revenues.agg(sum("Revenue").cast("long")).first.getLong(0)

    writeToFile(total)
  }

  def writeToFile(total:Long): Unit ={
    // Writing to a text file
    Files.write(Paths.get("TotalRevenue.txt"), total.toString.getBytes(StandardCharsets.UTF_8))
  }


}
