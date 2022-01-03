package com.project.traits
import org.apache.spark.sql

/**
 *
 * @tparam data
 * The trait ProductType is effectuated by the ProductTypeCounter class
 */
trait ProductType[data] {
  /**
   *
   * @param productLine(Enum)
   * @return Number of Product types under the Product Line Golf Equipment to Invoker class(Long)
   * Creates sql query to select product types under product line Golf Equipment
   */
  def queryCreator(productLine: data):Long

  /**
   *
   * @param productType(Dataframe)
   * @return Number of Product types under the Product Line Golf Equipment to queryCreator method(Long)
   * Counts the number of product types under the Product Line Golf Equipment
   */
  def countProductTypes(productType:sql.DataFrame):Long

  /**
   *
   * @param productType(Dataframe)
   * @param path(String)
   * Writes the product types into parquet file
   */
  def writeAsParquet(productType:sql.DataFrame,path:String)

}
