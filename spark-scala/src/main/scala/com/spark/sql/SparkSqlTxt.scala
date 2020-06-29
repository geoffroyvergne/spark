package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.Logger

object SparkSqlTxt {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(SparkSqlTxt.getClass)

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {

    logger.info("SparkSql")

    val conf = new SparkConf()
      .setAppName("SparkSqlTxt")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val people = sc.textFile("src/main/resources/people.txt")
      .map(_.split(","))
      .map(p => Person(p(0), p(1).trim.toInt))
      .toDF()

    people.registerTempTable("people")

    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")

    teenagers
      .map(row => "Name: " + row(0))
      .collect()
      .foreach(logger.info)

    teenagers
      .map(row => "Name: " + row.getAs[String]("name"))
      .collect()
      .foreach(logger.info)

    /*teenagers
      .map(_.getValuesMap[Any](List("name", "age")))
      .collect
      .foreach(element => logger.info(element.toString))*/


    //save dataframe to parket format
    //people.select("name", "age").write.format("parquet").save("src/main/resources/namesAndAges.parquet")

  }
}
