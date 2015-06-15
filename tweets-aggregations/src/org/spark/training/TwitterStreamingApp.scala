package org.spark.training

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext._
import org.joda.time.DateTime
import org.apache.spark.streaming.Minutes

object TwitterStreamingApp extends App {

  System.setProperty("twitter4j.oauth.consumerKey", "consumerKey")
  System.setProperty("twitter4j.oauth.consumerSecret", "consumerSecret")
  System.setProperty("twitter4j.oauth.accessToken", "accessToken")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "accessTokenSecret")

  val sparkConf = new SparkConf()
    .setMaster("master")
    .setAppName("twitter-spark-streaming-example")

  val ssc = new StreamingContext(sparkConf, Seconds(180))
  val stream = TwitterUtils.createStream(ssc, None)

  stream.map(status => (Utils.getTwitterSource(status.getSource), 1))
    .transform(
      _.reduceByKey(_ + _)
        .sortBy(_._2, ascending = false))
    .saveAsTextFiles("hdfs://ip:port/test", "")

  stream.map(status => (new DateTime(status.getCreatedAt).getMinuteOfDay, 1))
    .transform(
      _.reduceByKey(_ + _)
        .sortBy(_._2, ascending = false))
    .saveAsTextFiles("hdfs://ip:port/test", "")

  ssc.start()
  ssc.awaitTermination()
}