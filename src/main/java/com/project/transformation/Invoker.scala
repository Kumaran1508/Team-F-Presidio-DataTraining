package com.project.transformation

import com.project.enumloader.{ProductLineExtractor, RetailerCountryExtractor}

object Invoker {
  def main(args: Array[String]): Unit = {

    val countProductType = new ProductTypeCounter
    val noOfProductTypes = countProductType.queryCreator(ProductLineExtractor.ProductLine)

    //Counting number of product types
    println("No.of Product Types for the Product Line Golf Equipment ==>"+noOfProductTypes)

    val totalRevenue = new TotalRevenueCalculator
    val totalRevenueforFrance = totalRevenue.queryCreator(RetailerCountryExtractor.retailerCountry)

    //Calculating total revenue of the Retailer Country France
    println("Total revenue of the Retailer Country France==>"+totalRevenueforFrance)

  }
}
