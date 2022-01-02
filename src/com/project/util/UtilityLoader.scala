package com.project.util
import org.apache.spark.sql.SparkSession
trait UtilityLoader {

    //Loading winutils
    System.setProperty("hadoop.home.dir", "D:\\Downloads\\Presidio Data Training\\Winutils")

    //Creation of Spark Session
    val sparkSession = SparkSession.builder()
      .appName("CodaDataJOB")
      .master("local[*]")
      .getOrCreate()

    //Reading CSV
    val csvDF = sparkSession.read
    .option("header", "true")
    .csv("D:\\Downloads\\sales.csv")

    //Creation of Table
    csvDF.createOrReplaceTempView("SALES_TABLE")

}
