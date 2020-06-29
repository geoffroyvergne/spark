package com.spark.mllib

import org.slf4j.Logger

object SparkMllib {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(SparkMllib.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("SparkMllib")
  }
}
