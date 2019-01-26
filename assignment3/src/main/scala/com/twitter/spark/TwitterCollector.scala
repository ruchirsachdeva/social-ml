import java.util.Date

import com.google.gson.Gson
import com.twitter.spark.mllib.MLDataAnalysis
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.{Status, Twitter, TwitterFactory}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.AccessToken



/**
  * Continuously collect statuses from Twitter and save them into HDFS or S3.
  *
  * Tweets are partitioned by date such that you can create a partitioned Hive table over them.
  */

//BI6IVrrX9S3PB0lBtnju9Dq1G
//5QBDVOzRZ2IDjxtxLoyl0dPxoq2xqdB0oDcpHHM1gxus1Wb0oS
//41644104-e7ObeMxiEOAkZ3PUu7cil2PhnglgV4wLjRKFuUVML
//sQvRCQVlnQ3jbR9ddVGJnRjGrgvvXGmz5fxjLsqbyZT3u

object TwitterCollector {
  val gson = new Gson()
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


    // Setting up streaming context with a window of 10 seconds
    val sparkConf = new SparkConf().setMaster("local[12]").setAppName("Streaming Twitter")

    val ssc = new StreamingContext(sparkConf, Seconds(60))

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


    val jsonStatuses  = statuses.map(s => gson.toJson(s))

    def printToFile(formattedStatuses: DStream[String], fileType: String) = {
      // Group into larger batches
      val batchedStatuses = formattedStatuses.window(Seconds(outputBatchInterval), Seconds(outputBatchInterval))

      // Coalesce each batch into fixed number of files
      val coalesced = batchedStatuses.transform(rdd => rdd.coalesce(outputFiles))

      // Save as output in correct directory
      coalesced.foreachRDD((rdd, time) =>  {
        println("Inside rdd------ ")
        val outPartitionFolder = outDateFormat.format(new Date(time.milliseconds))
        rdd.saveAsTextFile("%s/%s".format(outputDir+"/"+fileType, outPartitionFolder))
      })
    }



    printToFile(jsonStatuses, "json")
 //   printToFile(formattedStatuses, "hive")

    ssc.start()
    ssc.awaitTermination()
  }


}