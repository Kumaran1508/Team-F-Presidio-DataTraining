package com.project.transformation
import com.project.enumloader.RetailerCountryExtractor
import com.project.util.UtilityLoader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.project.traits.TotalRevenue
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import com.project.enumloader.RetailerCountryExtractor.{RetailerCountryExtractor}
/**
   * @author Kumaran
   *         kumarans1508@gmail.com
   *         
   * Extracts Revenue of Retailer France        
   * @param RetailerCountry
   */
class TotalRevenueCalculator extends UtilityLoader with TotalRevenue[RetailerCountryExtractor]{
  override def queryCreator(RetailerCountry: RetailerCountryExtractor.Value): Double = {
    //Extraction of Revenue of Retailer France
    val totalRevenue = sparkSession.sql("SELECT Revenue FROM SALES_TABLE WHERE RetailerCountry='"+RetailerCountry+"'")

    calculateRevenue(totalRevenue)
  }
  /**
   *
   * @param revenues
   * calculates total revenue of the country France
   *
   */

  override def calculateRevenue(revenues : DataFrame): Double ={
    //calculating total revenue of the country France
    val total = revenues.agg(sum("Revenue").cast("long")).first.getLong(0)
    writeToFile(total)
    total
  }
   /**
   *
   * @param total
   * Writes the result to a text file
   *
   */
  override def writeToFile(total:Long): Unit ={
    // Writing to a text file
    Files.write(Paths.get("TotalRevenue.txt"), total.toString.getBytes(StandardCharsets.UTF_8))
  }


}
