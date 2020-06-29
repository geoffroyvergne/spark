package com.spark.core

import scala.math.random
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.Logger

object SparkPi {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(SparkPi.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("SparkPi")

    val conf = new SparkConf().setAppName("SparkPi").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val slices = 2

    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow

    val count = sc.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)

    logger.info("Pi is roughly " + 4.0 * count / (n - 1))
  }
}
