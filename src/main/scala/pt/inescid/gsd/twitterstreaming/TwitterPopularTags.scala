/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pt.inescid.gsd.twitterstreaming

import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

import scala.util.Random

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */
object TwitterPopularTags {
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

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val distFile = ssc.textFileStream("/home/sesteves/twitter-spark-example/twitter-dump")

    // val hashTags = distFile.flatMap(status => status.split(" ").filter(_.startsWith("#")))

    // clustering or census sampling techniques
    val words = distFile.flatMap(_.split(" ")).filter(_.length > 3).filter((word) => {Random.nextInt(2) == 0} )


    val wordCharValues = words.map(word => {
      var sum = 0
      word.toCharArray.foreach(sum += _.toInt)
      (sum.toDouble / word.length.toDouble, 1)
    })
      .reduceByWindow({ case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)}, Seconds(10), Seconds(10))

    wordCharValues.foreachRDD(rdd => {
      val result = rdd.take(1)
      val topList = rdd.take(10)

      println("Result array size: " + result.size)
      if(result.size > 0)
        println("Average word char value: %f".format((result(0)._1.toDouble / result(0)._2.toDouble)))

      topList.foreach(println)
    })

//    COUNT
//    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(_+_, Seconds(10))
//      .map{case (word, count) => (count, word)}.transform(_.sortByKey(false))
//    wordCounts.foreachRDD(rdd => {
//      val topList = rdd.take(10)
//      println("Popular words in the last 10 seconds (%s total)".format(rdd.count()))
//      topList.foreach(println)
//    })




//    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
//      .map { case (topic, count) => (count, topic)}
//      .transform(_.sortByKey(false))

//    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
//      .map { case (topic, count) => (count, topic)}
//      .transform(_.sortByKey(false))


    // Print popular hashtags
//    topCounts60.foreachRDD((rdd, time) => {
//      val topList = rdd.take(10)
//      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
//      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count))}
//    })

//    topCounts10.foreachRDD((rdd, time) => {
//      val topList = rdd.take(10)
//      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
//      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count))}
//    })

    ssc.start()
    ssc.awaitTermination()
  }
}
