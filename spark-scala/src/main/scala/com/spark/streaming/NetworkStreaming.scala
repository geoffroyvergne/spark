package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.Logger

/**
  * To run this on your local machine, you need to first run a Netcat server
  * netcat -lp 9999
  * type words
  */
object NetworkStreaming {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(NetworkStreaming.getClass)

  val hostname = "localhost"
  val port: Int = 9999

  def main(args: Array[String]): Unit = {

    logger.info("NetworkStreaming")

    val sparkConf = new SparkConf().setAppName("NetworkStreaming").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val lines = ssc.socketTextStream(hostname, port, StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines
      .flatMap(_.split(" "))

    val wordCounts = words
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
