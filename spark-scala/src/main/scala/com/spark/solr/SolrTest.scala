package com.spark.solr

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.Logger

object SolrTest {
  val logger: Logger = org.slf4j.LoggerFactory.getLogger(SolrTest.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("SolrTest")

    val conf = new SparkConf().setAppName("SolrTest Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
  }
}
