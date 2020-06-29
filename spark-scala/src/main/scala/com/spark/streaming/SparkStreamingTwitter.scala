package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.Logger

object SparkStreamingTwitter {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(SparkStreamingTwitter.getClass)


  def main(args: Array[String]): Unit = {

    logger.info("SparkStreamingTwitter")

    TwitterAuth.auth()

    val sparkConf = new SparkConf().setAppName("SparkStreamingTwitter").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    TwitterUtils
      .createStream(ssc, None)
      //.map(status => status)
      .filter(status => status.getLang().toLowerCase().equals("en"))
      //.map(status => status.getUser)
      .countByValue()
      //.count()
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
