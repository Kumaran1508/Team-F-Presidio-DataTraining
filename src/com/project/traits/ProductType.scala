package com.project.traits

import com.project.enumloader.ProductLineEnum
import org.apache.spark.sql

trait ProductType {
  def queryCreator(productLine: ProductLineEnum)
  def countProductTypes(productType:sql.DataFrame)

}
