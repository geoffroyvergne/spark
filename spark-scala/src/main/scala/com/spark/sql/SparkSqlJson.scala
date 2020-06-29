package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.Logger

object SparkSqlJson {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(SparkSqlJson.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("SparkSql")

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.json("src/main/resources/employee.json")
    df.registerTempTable("employee")

    df.show()
    df.printSchema()
    df.select("name").show()
    df.select(df("name"), df("age") + 1).show()
    df.filter(df("age") > 21).show()
    df.groupBy("age").count().show()

    sqlContext.sql("SELECT * FROM employee").show()
  }
}
