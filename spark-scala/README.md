# spark-scala

## compile : 
```sbt package```

## launch : 

```
bin/spark-submit \
  --class "org.spark.simple.SimpleApp" \
  --master local[4] \
  target/scala-2.11/spark-scala_2.11-1.0.jar
```

```
bin/spark-submit \
  --class "org.spark.simple.PiApp" \
  --master local[4] \
  target/scala-2.11/spark-scala_2.11-1.0.jar
```

## Twitter acces : 

Add this object : 

```
object TwitterAuth {

  def auth(): Unit = {
    System.setProperty("twitter4j.oauth.consumerKey", "*****************************")
    System.setProperty("twitter4j.oauth.consumerSecret", "*****************************")
    System.setProperty("twitter4j.oauth.accessToken", "*****************************")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "*****************************")
  }
}
```             
