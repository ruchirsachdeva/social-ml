package com.twitter.spark

import java.util.Date

import com.google.gson.Gson
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.{Place, Status, TwitterFactory}
import twitter4j.auth.AccessToken
import twitter4j.conf.ConfigurationBuilder


/**
  * Continuously collect statuses from Twitter and save them into HDFS or S3.
  *
  * Tweets are partitioned by date such that you can create a partitioned Hive table over them.
  */

//BI6IVrrX9S3PB0lBtnju9Dq1G
//5QBDVOzRZ2IDjxtxLoyl0dPxoq2xqdB0oDcpHHM1gxus1Wb0oS
//41644104-e7ObeMxiEOAkZ3PUu7cil2PhnglgV4wLjRKFuUVML
//sQvRCQVlnQ3jbR9ddVGJnRjGrgvvXGmz5fxjLsqbyZT3u

object TwitterCollectorAnalyzer {
  val gson = new Gson()

  // Setting up streaming context with a window of 10 seconds
  val sparkConf = new SparkConf().setMaster("local[12]").setAppName("Streaming Twitter")

  val ssc = new StreamingContext(sparkConf, Seconds(60))
  val sc = ssc.sparkContext
  val sqlContext = new SQLContext(sc)
  //to convert $'col name' into an Column
  import sqlContext.implicits._

  def main(args: Array[String]) {
    val consumerKey = "BI6IVrrX9S3PB0lBtnju9Dq1G"
    val consumerSecret = "5QBDVOzRZ2IDjxtxLoyl0dPxoq2xqdB0oDcpHHM1gxus1Wb0oS"
    val accessToken = "41644104-e7ObeMxiEOAkZ3PUu7cil2PhnglgV4wLjRKFuUVML"
    val accessTokenSecret = "sQvRCQVlnQ3jbR9ddVGJnRjGrgvvXGmz5fxjLsqbyZT3u"

    // Authorizing with your Twitter Application credentials
    val cb = new ConfigurationBuilder()
    cb.setJSONStoreEnabled(true);

    val twitter = new TwitterFactory(cb.build()).getInstance();
    twitter.setOAuthConsumer(consumerKey, consumerSecret)
    twitter.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret))


    // Local directory for stream checkpointing (allows us to restart this stream on failure)
    val checkpointDir = sys.env.getOrElse("CHECKPOINT_DIR", "D:/tmp").toString

    // Output directory
    val outputDir = sys.env.getOrElse("OUTPUT_DIR", "D:/tmp/tweets_out")

    // Size of output batches in seconds
    val outputBatchInterval = sys.env.get("OUTPUT_BATCH_INTERVAL").map(_.toInt).getOrElse(60)

    // Number of output files per batch interval.
    val outputFiles = sys.env.get("OUTPUT_FILES").map(_.toInt).getOrElse(1)

    // Echo settings to the user
    Seq(("CHECKPOINT_DIR" -> checkpointDir),
      ("OUTPUT_DIR" -> outputDir),
      ("OUTPUT_FILES" -> outputFiles),
      ("OUTPUT_BATCH_INTERVAL" -> outputBatchInterval)).foreach {
      case (k, v) => println("%s: %s".format(k, v))
    }


    System.setProperty("spark.cleaner.ttl", (outputBatchInterval * 5).toString)
    System.setProperty("spark.cleaner.delay", (outputBatchInterval * 5).toString)

    // Date format for creating Hive partitions
    val outDateFormat = outputBatchInterval match {
      case 60 => new java.text.SimpleDateFormat("yyyy/MM/dd/HH/mm")
      case 3600 => new java.text.SimpleDateFormat("yyyy/MM/dd/HH")
    }

    val statuses  = TwitterUtils.createStream(ssc, Option(twitter.getAuthorization()),
      Seq("Narendra Modi","NarendraModi","Rahul Gandhi","RahulGandhi","lok sabha elections", "loksabha elections", "lok sabha", "loksabha", "election 2019 india", "Bharatiya Janta Party","BharatiyaJantaParty", "BJP", "Indian National Congress","INC" ,"IndianNationalCongress", "Congress india"))

  //    val formattedStatuses = statuses.map(s => formatStatus(s))



    def printToFile(formattedStatuses: DStream[Status], fileType: String) = {
      // Group into larger batches
      val batchedStatuses = formattedStatuses.window(Seconds(outputBatchInterval), Seconds(outputBatchInterval))

      // Coalesce each batch into fixed number of files
      val coalesced = batchedStatuses.transform(rdd => rdd.coalesce(outputFiles))


      coalesced.foreachRDD { rdd =>
        println("analyze rdd begin")
        if (!rdd.isEmpty()) {
          println("analyze rdd")
          val tweetsRDD = rdd.map(twt=>(twt.getId, twt.getCreatedAt().toString(),twt.getText(), parseSource(twt.getSource()),twt.getLang(),twt.getUser().getLang(),parsePlace(twt.getPlace()), twt.getRetweetCount))
           val tweetsDF = tweetsRDD.toDF("id", "createdAt","text","source","lang","userLang","place", "retweetCount")
           // val tweetsDF = sqlContext.createDataFrame(tweetsRDD)

          tweetsDF.printSchema()
          tweetsDF.show()
          analyzeDataFrame(tweetsDF)
        }
        println("end")

      }
      println("end rdd")

    }








    printToFile(statuses, "json")
 //   printToFile(formattedStatuses, "hive")

    ssc.start()
    ssc.awaitTermination()
  }
  //Getting the source name from link <a ....>source name</a>
  def parseSource(source:String):String = {
    if (source != null){
      val srArr = source.split(">")
      if(srArr.length>1){
        return srArr(1).split("<")(0)

      }
      else {
        return "Other"
      }
    }
    else {
      return "Other"
    }
  }

  //Getting the place name if place is not null
  def parsePlace(place:Place):String = {

    if (place != null){
      val placeName = place.getName()
      if(placeName != null){
        return placeName

      }
      else {
        return "Other"
      }
    }
    else {
      return "Other"
    }
  }


  def analyzeDataFrame(tweetsDF: DataFrame) {
    //Load tweets as Dataframe to Spark
    // val tweetsDF = sqlContext.read.json(path)

    // "D:/tmp/tweets_out/json/2019/01/12/17"
    //  val tweetsDF = sqlContext.read.json("src/main/resources/json-files/tweets.json")


    /*Cleaning the data*/
    println("---------------------Filter tweets by language --------------------------------")
    //Filter tweets by language
    val englishTweets = tweetsDF.filter("lang == 'en'")
    englishTweets.select("text").show(5)

    println("---------------------Filter tweets by device/source --------------------------------")
    //Filter out records with bad or missing values
    val tweetsWithAllSources = englishTweets.filter(col("source").isNotNull)
    tweetsWithAllSources.select("text").show(5)


    //Filter tweets by device
    val sources = Array("Twitter for iPhone", "Twitter for Android", "Twitter Web Client", "Twitter for iPad")
    val tweetsWithoutBot = tweetsWithAllSources.filter(col("source").contains(sources(0)) || col("source").contains(sources(1)) || col("source").contains(sources(2)) || col("source").contains(sources(3)))

    //tweetsWithoutBot.select(expr("(split((split(source, '>'))[1],'<'))[0]").cast("string").as("sourceName")).groupBy("sourceName").count().sort($"count".desc).show(15, false)
    tweetsWithoutBot.select("text").show(5)
    println("---------------------Extracting useful features from the data --------------------------------")
    //Create new columns based on existing ones


    //Select interested data
    val data = tweetsWithoutBot.select(col("createdAt"), col("retweetCount"), col("source"), col("id"), col("userLang"), col("text"))

    val extractHashtags = udf((message: String) => {
      val words = message.split(" ")
      words.filter(word => word.startsWith("#"))
    })
    val tweetsWithHashtags = data.withColumn("hashtags", extractHashtags(col("text")))
    tweetsWithHashtags.select("hashtags").take(5).foreach(println)

    println("---------------------Dealing with missing values --------------------------------")

    //Fill in bad or missing data by replacing null values in dataframe.
    val cleanedData = tweetsWithHashtags.na.fill("Other", Seq("timeZone"))


    //Text features

    //Tokanization
    //Tokenization is the process of taking text (such as a sentence) and breaking it into individual terms (usually words).
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val tokenized = tokenizer.transform(cleanedData)
    tokenized.select("words", "text").take(3).foreach(println)
    //
    //Stop Words Removal

    //    //Stop words are words which should be excluded from the input, typically because the words appear frequently and don’t carry as much meaning.
    //    //Default stop words for some languages are accessible by calling StopWordsRemover.loadDefaultStopWords(language), for which available options are “danish”, “dutch”, “english”, “finnish”, “french”, “german”, “hungarian”, “italian”, “norwegian”, “portuguese”, “russian”, “spanish”, “swedish” and “turkish”. A boolean parameter caseSensitive indicates if the matches should be case sensitive (false by default).
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")
    val stopWordsRemoved = remover.transform(tokenized)
    stopWordsRemoved.select("text", "words", "filtered_words").take(3).foreach(println)

    println("---------------------Data Representation--------------------------------")
    //
    //The Word2VecModel transforms each document into a vector using the average of all words in the document; this vector can then be used as features for prediction, document similarity calculations
    val word2Vec = new Word2Vec().setInputCol("filtered_words").setOutputCol("features").setVectorSize(5).setMinCount(0)
    val model = word2Vec.fit(stopWordsRemoved)
    val transformedData = model.transform(stopWordsRemoved)
    val preparedData = transformedData.distinct
    preparedData.select("text", "features").take(3).foreach(println)
    //
    var outputSuffix=1
    val output = "src/main/resources/output/"+ outputSuffix
    outputSuffix +=1
    preparedData.coalesce(1).write.json(output + "/features")
    println("---------------------Tweets Clustering--------------------------------")
    //k-means clsutering
    //train the model
    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = preparedData.randomSplit(Array(0.7, 0.3), seed = 1234L)



    val kVal = 75
    val kValWSSSE: Array[Array[Double]] = Array.ofDim[Double](50, 2)
    var count=0
    // Calculate WSSSE for a set of kVal and store in kValWSSSE
    for (kVal <- Seq(20,  30,  40,  50,  60,  70,  80, 90, 100, 120, 140, 300)) {
      //    // Trains a k-means model.
      val kmeans = new KMeans().setK(kVal).setSeed(1L)
      val kmeansModel = kmeans.fit(trainingData.cache())

      // Evaluate clustering by computing Within Set Sum of Squared Errors.
      val WSSSE = kmeansModel.computeCost(preparedData)
      println(s"kVal = $kVal , WSSSE = $WSSSE")
      kValWSSSE(count)(0) = kVal
      kValWSSSE(count)(1) =WSSSE
      count += 1
    }


    // calculate optimal k-value using elbow method,
    // where rate of change of WSSSE is very less with increasing k value
    var elbowKval: Int = kValWSSSE(0)(0).toInt
    for (i <- 0 until kValWSSSE.length-1) {

      val y1: Double = kValWSSSE(i)(1)
      val y2: Double = kValWSSSE(i + 1)(1)
      val x1: Int = kValWSSSE(i)(0).toInt
      val x2: Int = kValWSSSE(i + 1)(0).toInt

      // find rate of change of slope between 2 consecutive points
      val rateOfChangeOfSlope: Double = (y2 - y1) / (x2 - x1)

      // elbowKval is the point after which the rate of change of slope stays very less
      if (rateOfChangeOfSlope < (-3)) {
        elbowKval = x2
      }
    }

    println(s"elbowKval $elbowKval")

    //    // Trains a k-means model.
    val kmeans = new KMeans().setK(elbowKval).setSeed(1L)
    val kmeansModel = kmeans.fit(trainingData.cache())

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = kmeansModel.computeCost(preparedData)
    println(s"$elbowKval $WSSSE")
    // Shows the result.
    println("Cluster Centers: ")
    kmeansModel.clusterCenters.foreach(println)

    // Select example rows to display.
    val kmeans_predictions = kmeansModel.transform(testData.cache())
    //kmeans_predictions.show()

    //What are the biggest clusters? In this example:0,7,17
    kmeans_predictions.select("prediction").groupBy("prediction").count().sort($"count".desc).show(20)


    //save results in json file
    kmeans_predictions.coalesce(1).write.json(output + "/kmeans_result")
    //Visualizaiton in Zepplin

    println("---------------------Tweets Classification--------------------------------")

    //Sentiment Analysis
    // 6 classes:Very negative, Negative,Neutral, Positive,Very positive, Not understood
    //Create Labled Data by using the Stanford Natural Language Processing Group in order extract the corresponding sentiments.
    val detectSentiment = udf((message: String) => {

      val sentiment = SentimentAnalysisUtils.detectSentiment(message)
      sentiment.toString()
    })
    val trainingDataWithSentiment = trainingData.withColumn("sentiment",detectSentiment(col("text")))

    val sentimentIndex = udf((sentiment_type:String)=>{
      val index = sentiment_type match {
        case  "VERY_NEGATIVE" => 0
        case  "NEGATIVE" =>1
        case  "NEUTRAL" =>2
        case  "POSITIVE" =>3
        case  "VERY_POSITIVE" =>4
        case  "NOT_UNDERSTOOD" =>5
      }
      index
    })

    val trainingLabledData = trainingDataWithSentiment.withColumn("label",sentimentIndex(col("sentiment")))
    trainingLabledData.coalesce(1).write.json(output + "/trainig_labled_bayes")

    //load the labled data
    //   val trainingLabledData = sqlContext.read.json("src/main/resources/output/trainig_labled_bayes")

    //TF-IDF for training data
    val hashingTF = new HashingTF().setInputCol("filtered_words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(trainingLabledData.cache())

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("bayes_features")
    val idfModel = idf.fit(featurizedData)
    val trainigFeaturedData = idfModel.transform(featurizedData)

    trainigFeaturedData.printSchema()
    // Train a NaiveBayes model.
    val bayes_model = new NaiveBayes().setFeaturesCol("bayes_features").fit(trainigFeaturedData)


    //TF-IDF for testing data
    val hashingTF2 = new HashingTF().setInputCol("filtered_words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData2 = hashingTF2.transform(testData.cache())

    val idf2 = new IDF().setInputCol("rawFeatures").setOutputCol("bayes_features")
    val idfModel2 = idf2.fit(featurizedData2)
    val testingFeaturedData = idfModel2.transform(featurizedData2)


    // Select example rows to display.
    bayes_model.setPredictionCol("prediction_bayes")

    val bayes_predictions = bayes_model.transform(testingFeaturedData)
    bayes_predictions.take(3).foreach(println)
    bayes_predictions.printSchema()

    //save results in json file
    bayes_predictions.coalesce(1).write.json(output + "/bayes_result")
    //Visualizaiton in Zepplin

  }


}



import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object SentimentAnalysisUtils {

  val nlpProps = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }

  def detectSentiment(message: String): SENTIMENT_TYPE = {

    val pipeline = new StanfordCoreNLP(nlpProps)

    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()

    var longest = 0
    var mainSentiment = 0

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val partText = sentence.toString

      if (partText.length() > longest) {
        mainSentiment = sentiment
        longest = partText.length()
      }

      sentiments += sentiment.toDouble
      sizes += partText.length

      // println("debug: " + sentiment)
      // println("size: " + partText.length)

    }

    val averageSentiment:Double = {
      if(sentiments.size > 0) sentiments.sum / sentiments.size
      else -1
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))

    if(sentiments.size == 0) {
      mainSentiment = -1
      weightedSentiment = -1
    }


    // println("debug: main: " + mainSentiment)
    //println("debug: avg: " + averageSentiment)
    // println("debug: weighted: " + weightedSentiment)

    /*
     0 -> very negative
     1 -> negative
     2 -> neutral
     3 -> positive
     4 -> very positive
     */
    weightedSentiment match {
      case s if s <= 0.0 => NOT_UNDERSTOOD
      case s if s < 1.0 => VERY_NEGATIVE
      case s if s < 2.0 => NEGATIVE
      case s if s < 3.0 => NEUTRAL
      case s if s < 4.0 => POSITIVE
      case s if s < 5.0 => VERY_POSITIVE
      case s if s > 5.0 => NOT_UNDERSTOOD
    }

  }

  trait SENTIMENT_TYPE
  case object VERY_NEGATIVE extends SENTIMENT_TYPE
  case object NEGATIVE extends SENTIMENT_TYPE
  case object NEUTRAL extends SENTIMENT_TYPE
  case object POSITIVE extends SENTIMENT_TYPE
  case object VERY_POSITIVE extends SENTIMENT_TYPE
  case object NOT_UNDERSTOOD extends SENTIMENT_TYPE

}