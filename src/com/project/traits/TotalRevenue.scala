package com.project.traits
import com.project.enumloader.RevenueEnum
import org.apache.spark.sql.DataFrame

trait TotalRevenue {
def queryCreator(retailerCountry: RevenueEnum.Value)
  def calculateRevenue(revenues: DataFrame)
}
