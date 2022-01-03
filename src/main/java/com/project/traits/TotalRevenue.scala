package com.project.traits
import org.apache.spark.sql.DataFrame
  /**
   * @author Meghana
   *         megha13121@gmail.com
   *         
   */

/**
 *
 * @tparam data
 * The trait TotalRevenue is effectuated by TotalRevenueCalculator class
 */
trait TotalRevenue[data] {
  /**
   *
   * @param retailerCountry(Enum)
   * @return TotalRevenue of the Retailer Country to Invoker class(Double)
   * Creates sql query to select all the revenues under the Retailer Country France
   */
  def queryCreator(retailerCountry: data):Double

  /**
   *
   * @param revenues(Dataframe)
   * @return TotalRevenue of the retailer Country to queryCreator method(Double)
   * Calculates the total revenue of the Retailer Country France
   */
  def calculateRevenue(revenues: DataFrame):Double

  /**
   *
   * @param total(Long)
   * Writes the calculated total revenue to a file
   */
  def writeToFile(total:Long)
}



