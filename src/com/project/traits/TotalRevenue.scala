package com.project.traits

import org.apache.spark.sql.DataFrame

trait TotalRevenue {
def queryCreator(RetailerCountry: RevenueEnum.Value)
  def calculateRevenue(revenues: DataFrame)
}
