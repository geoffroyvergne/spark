package com.spark.streaming

import com.spark.sql.SparkSqlJson
import org.slf4j.Logger

object SparkStreaming {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(SparkSqlJson.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("SparkStream")
  }

}
