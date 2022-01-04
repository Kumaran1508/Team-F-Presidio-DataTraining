package com.project.util
import org.apache.spark.sql.SparkSession
import scala.io.Source
/**
 * UtilityLoader is effectuated by ProductTypeCounter and TotalRevenueCalculator to obtain the spark session and csv
 */
class UtilityLoader {

    //Creation of Spark Session
    val sparkSession = SparkSession.builder()
      .appName("SalesDataAnalysis")
      .master("spark://priya:7077")
      .getOrCreate()

    //Reading CSV
    import sparkSession.implicits._
    val csvRead=for {
        line <- Source.fromFile("/home/priya/Downloads/sales.csv").getLines().drop(1).toVector
        values = line.split(",")
    } yield Salesdata(values(0), values(1), values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9),values(10),values(11),values(12),values(13))
    val csvDF = csvRead.toDF()
    //Creation of Table
    csvDF.createOrReplaceTempView("SALES_TABLE")
}
case class Salesdata(
                      Year: String,
                      ProductLine: String,
                      ProductType: String,
                      Product: String,
                      OrderMethodType: String,
                      RetailerCountry:String,
                      Revenue:String,
                      PlannedRevenue:String,
                      ProductCost:String,
                      Quantity:String,
                      UnitCost:String,
                      UnitPrice:String,
                      GrossProfit:String,
                      UnitSalePrice:String
                    )
