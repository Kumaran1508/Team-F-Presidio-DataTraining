package com.project.transformation

import com.project.enumloader.RevenueEnum
import com.project.util.UtilityLoader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.project.traits.TotalRevenue
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class TotalRevenue extends UtilityLoader with TotalRevenue{
  /**
   * @author Kumaran
   *         kumarans1508@gmail.com
   *         
   * Extracts Revenue of Retailer France        
   * @param RetailerCountry
   */
  override def queryCreator(RetailerCountry: RevenueEnum.Value): Unit = {
    val totalRevenue = sparkSession.sql("SELECT Revenue FROM SALES_TABLE WHERE RetailerCountry='"+RetailerCountry+"'")

    calculateRevenue(totalRevenue)
  }

  /**
   *
   * @param revenues
   * calculates total revenue of the country France
   *
   */
  override def calculateRevenue(revenues : DataFrame): Unit ={
    val total = revenues.agg(sum("Revenue").cast("long")).first.getLong(0)
    println("Total revenue of the Retailer Country France==>"+total)
    writeToFile(total)
  }

  /**
   *
   * @param total
   * Writes the result to a text file
   *
   */
  override def writeToFile(total:Long): Unit ={
    Files.write(Paths.get("TotalRevenue.txt"), total.toString.getBytes(StandardCharsets.UTF_8))
  }


}
