package com.spark.simple

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.slf4j.Logger

object SimpleApp {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(SimpleApp.getClass)

  def main(args: Array[String]) {

    logger.info("SimpleApp")

    val logFile = "src/main/resources/README.md"
    
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()

    logger.info("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
