package com.spark.mllib.spamdetection

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.slf4j.Logger

object SpamDetection {
  val logger: Logger = org.slf4j.LoggerFactory.getLogger(SpamDetection.getClass)
  def main(args: Array[String]): Unit = {
    logger.info("SpamDetection")

    val conf = new SparkConf()
      .setAppName("SpamDetection")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val spam = sc.textFile("src/main/resources/spamdetection/0052.2003-12-20.GP.spam.txt", 4)
    val normal = sc.textFile("src/main/resources/spamdetection/0022.1999-12-16.farmer.ham.txt", 4)

    val tf = new HashingTF(numFeatures = 10000)
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)

    trainingData.cache()

    val model = new LogisticRegressionWithSGD().run(trainingData)

    //Test on a positive example (spam) and a negative one (normal).
    val posTest = tf.transform("insurance plan which change your life ...".split(" "))
    val negTest = tf.transform("hi sorry yaar i forget tell you i cant come today".split(" "))

    logger.info("Prediction for positive test example: " + model.predict(posTest))
    logger.info("Prediction for negative test example: " + model.predict(negTest))
  }
}
