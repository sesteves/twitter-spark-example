package pt.inescid.gsd.twitterstreaming

import java.io._

import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */
object CollectTweets {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setMaster("spark://ginja-A1:7077").setAppName("TwitterPopularTags")
      .setJars(Array("target/twitter-spark-example-1.0-SNAPSHOT.jar"))

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    stream.map(status => status.getText).foreachRDD((rdd, time) => {
      val pw = new PrintWriter(new FileWriter("twitter-dump.txt", true))
      val tweets = rdd.collect()
      tweets.foreach(pw.println)
      pw.close()
    })

    //    var gson = new Gson()
    //
    //    val partitionsEachInterval = 10
    //    val outputDirectory = "."
    //    val numTweetsToCollect = 100
    //    var numTweetsCollected = 0L
    //
    //    stream.map(gson.toJson(_)).filter(!_.contains("boundingBoxCoordinates"))
    //
    //    stream.foreachRDD((rdd, time) => {
    //      val count = rdd.count()
    //      if (count > 0) {
    //        val outputRDD = rdd.repartition(partitionsEachInterval)
    //        outputRDD.saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
    //        numTweetsCollected += count
    //        if (numTweetsCollected > numTweetsToCollect) {
    //          System.exit(0)
    //        }
    //      }
    //    })

    ssc.start()
    ssc.awaitTermination()
  }
}
