package org.apache.spark.examples.ml

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * create by liush on 2018-1-31
  */
trait SparkCommant {
  val conf=new SparkConf()
  val spark:SparkSession=SparkSession.builder().appName(s"${this.getClass.getSimpleName}")
                        .master("local[*]").config(conf).getOrCreate()
  val sc=spark.sparkContext
  val sqlContext=spark.sqlContext


}
