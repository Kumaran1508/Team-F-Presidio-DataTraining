package com.project.transformation
object Invoker {
  def main(args: Array[String]): Unit = {
    val countProductType = new CountProductType
    countProductType.queryCreator()

    val totalRevenue = new TotalRevenue
    totalRevenue.queryCreator()
  }
}
