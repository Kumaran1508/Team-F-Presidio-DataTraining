package com.project.transformation
import com.project.util.UtilityLoader
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class CountProductType extends UtilityLoader {

  def queryCreator(): Unit ={
    //Extraction of Product Types under the Product line Golf Equipment
    val productType = sparkSession.sql("SELECT ProductType FROM SALES_TABLE WHERE ProductLine='Golf Equipment'")

    countProductTypes(productType)
  }

  def countProductTypes(productType:sql.DataFrame): Unit ={

    //Obtaining unique product types
    val productTypeRDD = productType.rdd.map(eachWord => (eachWord,1))
    val individualProductType = productTypeRDD.reduceByKey((x,y) => x)

    //Conversion of RDD[(Row,Int)] to RDD[Row]
    val productTypeRow = individualProductType.map(each => Row(each._1.toString()))

    //Conversion of RDD[Row] to DataFrame
    val schema = StructType(Array(StructField("ProductTypes", StringType,true)))
    val productTypeDF = sparkSession.createDataFrame(productTypeRow,schema)

    //path
    val path = "D:\\data\\Product-Types"

    //Writing product types to parquet file
    productTypeDF.write.parquet(path)

    //Reading product types from the parquet file
   val readProductTypes = sparkSession.read.parquet(path)
   println("Product Types for the Product Line Golf Equipment")
   readProductTypes.show()

    //Counting number of product types
   val noOfProductTypes = readProductTypes.count()
   println("No.of Product Types for the Product Line Golf Equipment ==>"+noOfProductTypes)

  }
}
