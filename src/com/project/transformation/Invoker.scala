package com.project.transformation

import com.project.enumloader.{ProductLineEnum, RevenueEnum}

object Invoker {
  def main(args: Array[String]): Unit = {
    val countProductType = new CountProductType
    countProductType.queryCreator(ProductLineEnum.ProductLine)
    val totalRevenue = new TotalRevenue
    totalRevenue.queryCreator(RevenueEnum.RetailerCountry)
  }
}
