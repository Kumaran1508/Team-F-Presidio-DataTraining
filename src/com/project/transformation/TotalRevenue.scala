package com.project.transformation

import com.project.enumloader.RevenueEnum
import com.project.util.UtilityLoader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class TotalRevenue extends UtilityLoader {
  def queryCreator(RetailerCountry: RevenueEnum.Value): Unit = {
    //Extraction of Revenue of Retailer France
    val totalRevenue = sparkSession.sql("SELECT Revenue FROM SALES_TABLE WHERE RetailerCountry='"+RetailerCountry+"'")

    calculateRevenue(totalRevenue)
  }

  def calculateRevenue(revenues : DataFrame): Unit ={
    //calculating total revenue of the country France
    val total = revenues.agg(sum("Revenue").cast("long")).first.getLong(0)
    println("Total revenue of the Retailer Country France==>"+total)
    writeToFile(total)
  }

  def writeToFile(total:Long): Unit ={
    // Writing to a text file
    Files.write(Paths.get("TotalRevenue.txt"), total.toString.getBytes(StandardCharsets.UTF_8))
  }


}
