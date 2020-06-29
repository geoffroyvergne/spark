package com.spark.mllib.titanic

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.Logger

object TitanicRefacto {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(Titanic.getClass)

  val resultPath = "src/main/resources/titanic/result"
  val modelPath = "src/main/resources/titanic/model"
  val testPath = "src/main/resources/titanic/test.csv"
  val trainPath = "src/main/resources/titanic/train.csv"
  val csvFormat = "com.databricks.spark.csv"

  def main(args: Array[String]): Unit = {
    logger.info("Titanic")

    val sqlContext: SQLContext = setSqlContext()
    val crossValidationModel: CrossValidatorModel = trainOrLoad(sqlContext)
    val result = testModel(sqlContext, crossValidationModel)
    saveResult(sqlContext, result)
  }

  def setSqlContext(): SQLContext = {
    val conf = new SparkConf()
      .setAppName("Titanic")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    sqlContext
  }

  def trainOrLoad(sqlContext: SQLContext): CrossValidatorModel = {
    if (Files.isDirectory(Paths.get(modelPath))) {
      logger.info("model already exists, dont train")

      loadModel()
    } else {
      logger.info("model already exists, let train")

      val dataFrame: DataFrame = loadTrainFile(sqlContext)
      val cv: CrossValidatorModel = trainModel(dataFrame)
      saveModel(cv, true)

      cv
    }
  }

  def loadTrainFile(sqlContext: SQLContext): DataFrame = {
    FileUtils.deleteDirectory(new File(resultPath))

    // We use the $ operator from implicit class StringToColumn
    import sqlContext.implicits._

    // Load the train.csv file as a DataFrame
    // Use the spark-csv library see https://github.com/databricks/spark-csv
    val csv = sqlContext.read.format(csvFormat)
      .option("header", "true")
      .load(trainPath)

    // spark-csv put the type StringType to each column
    csv.printSchema()

    // select only the useful columns, rename them and cast them to the right type
    val df = csv.select(
      $"Survived".as("label").cast(DoubleType),
      $"Age".as("age").cast(IntegerType),
      $"Fare".as("fare").cast(DoubleType),
      $"Pclass".as("class").cast(DoubleType),
      $"Sex".as("sex"),
      $"Name".as("name")
    )

    // verify the schema
    df.printSchema()

    // look at the data
    df.show()

    // show stats for each column
    df.describe(df.columns: _*).show()

    df
  }

  def trainModel(df: DataFrame): CrossValidatorModel = {
    // We replace the missing values of the age and fare columns by their mean.
    val select = df.na.fill(Map("age" -> 30, "fare" -> 32.2))

    // We will train our model on 75% of our data and use the 25% left for validation.
    val Array(trainSet, validationSet) = select.randomSplit(Array(0.75, 0.25))

    // The stages of our pipeline
    val sexIndexer = new StringIndexer().setInputCol("sex").setOutputCol("sexIndex")
    val classEncoder = new OneHotEncoder().setInputCol("class").setOutputCol("classVec")
    val tokenizer = new Tokenizer().setInputCol("name").setOutputCol("words")
    val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("hash")
    val vectorAssembler = new VectorAssembler().setInputCols(Array("hash", "age", "fare", "sexIndex", "classVec")).setOutputCol("features")
    val logisticRegression = new LogisticRegression()
    val pipeline = new Pipeline().setStages(Array(sexIndexer, classEncoder, tokenizer, hashingTF, vectorAssembler, logisticRegression))

    // We will cross validate our pipeline
    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)

    // Here are the params we want to validationPredictions
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(2, 5, 1000))
      .addGrid(logisticRegression.regParam, Array(1, 0.1, 0.01))
      .addGrid(logisticRegression.maxIter, Array(10, 50, 100))
      .build()

    crossValidator.setEstimatorParamMaps(paramGrid)

    // We will use a 3-fold cross validation
    crossValidator.setNumFolds(3)

    logger.info("Cross Validation")

    val cvModel = crossValidator.fit(trainSet)

    logger.info("Best model")

    for (stage <- cvModel.bestModel.asInstanceOf[PipelineModel].stages) {
      logger.info(stage.explainParams())
    }

    logger.info("Evaluate the model on the validation set.")

    val validationPredictions = cvModel.transform(validationSet)

    // Area under the ROC curve for the validation set
    val binaryClassificationEvaluator: BinaryClassificationEvaluator = new BinaryClassificationEvaluator()

    logger.info(s"${binaryClassificationEvaluator.getMetricName} ${binaryClassificationEvaluator.evaluate(validationPredictions)}")

    // We want to print the percentage of passengers we correctly predict on the validation set
    val total = validationPredictions.count()
    val goodPredictionCount = validationPredictions.filter(validationPredictions("label") === validationPredictions("prediction")).count()

    logger.info(s"correct prediction percentage : ${goodPredictionCount / total.toDouble}")

    // Lets make prediction on new data where the label is unknown
    logger.info("Predict validationPredictions.csv passengers fate")

    cvModel
  }

  def testModel(sqlContext: SQLContext, cvModel: CrossValidatorModel): DataFrame = {

    import sqlContext.implicits._

    val csvTest = sqlContext.read.format(csvFormat)
      .option("header", "true")
      .load(testPath)

    val dfTest = csvTest.select(
      $"PassengerId",
      $"Age".as("age").cast(IntegerType),
      $"Fare".as("fare").cast(DoubleType),
      $"Pclass".as("class").cast(DoubleType),
      $"Sex".as("sex"),
      $"Name".as("name")
    ).coalesce(1)

    val selectTest = dfTest.na.fill(Map("age" -> 30, "fare" -> 32.2))

    val result = cvModel.transform(selectTest)

    result
  }

  def loadModel(): CrossValidatorModel = {
    CrossValidatorModel.load(modelPath)
  }

  def saveModel(cvModel: CrossValidatorModel, overWrite: Boolean): Unit = {
    if(overWrite) {
      cvModel.write.overwrite().save(modelPath)
    } else {
      cvModel.save(modelPath)
    }
  }

  def saveResult(sqlContext: SQLContext, result: DataFrame): Unit = {
    FileUtils.deleteDirectory(new File(resultPath))

    import sqlContext.implicits._

    // let's write the result in the correct format for Kaggle
    result.select($"PassengerId", $"prediction".cast(IntegerType))
      .write
      .format(csvFormat)
      .save(resultPath)
  }
}
