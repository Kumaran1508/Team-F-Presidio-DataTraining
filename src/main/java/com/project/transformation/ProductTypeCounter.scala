package com.project.transformation
import com.project.enumloader.ProductLineExtractor
import com.project.enumloader.ProductLineExtractor.ProductLineExtractor
import com.project.traits.ProductType
import com.project.util.UtilityLoader
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * @author Priyadharsini(priyadhars192001@gmail.com)
 */
class ProductTypeCounter extends UtilityLoader with ProductType[ProductLineExtractor] {
  override def queryCreator(ProductLine: ProductLineExtractor.Value): Long ={
    //Extraction of Product Types under the Product line Golf Equipment
    val productType = sparkSession.sql("SELECT ProductType FROM SALES_TABLE WHERE ProductLine= '" + ProductLine + "'")

    countProductTypes(productType)
  }

  override def countProductTypes(productType:sql.DataFrame): Long ={

    //Obtaining unique product types
    val productTypeRDD = productType.rdd.map(productType => (productType,1))
    val individualProductType = productTypeRDD.reduceByKey((productType,count) => productType)

    //Conversion of RDD[(Row,Int)] to RDD[Row]
    val productTypeRow = individualProductType.map(productType => Row(productType._1.toString()))

    //Conversion of RDD[Row] to DataFrame
    val schema = StructType(Array(StructField("ProductTypes", StringType)))
    val productTypeDF = sparkSession.createDataFrame(productTypeRow,schema)

    //path
    val path = "/home/priya/Product-Types"

    //Writing to file
    writeAsParquet(productTypeDF,path)

    //Reading product types from the parquet file
    val readProductTypes = sparkSession.read.parquet(path)
    println("Product Types for the Product Line Golf Equipment")
    readProductTypes.show()

    //Counting the number of product types
    readProductTypes.count()

  }

  override def writeAsParquet(productTypeDF: DataFrame,path:String): Unit= {
    //Writing product types to parquet file
    productTypeDF.write.parquet(path)
  }
}
