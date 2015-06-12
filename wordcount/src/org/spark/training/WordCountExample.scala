package org.spark.training

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Spark word count example on messages from tweets used during Spark Intro Training 

object WordCountExample extends App {

  val masterIp = "instance-8836.bigstep.io"
  val twitterPath = "/user/training/input/twitter/small/twitter-stream-sample.json"
  val name = "andrei"

  val sparkConf = new SparkConf()
    .setMaster("spark://" + masterIp + ":7077")
    .set("spark.cores.max", "2")
    .set("spark.executor.memory", "1g")
    .setAppName("word-count-app")

  val sc = new SparkContext(sparkConf)
  sc.addJar("/home/training/" + name + "/wordcount.jar")

  // create RDD[String] from file where each string is a line
  val textFile = sc.textFile("hdfs://" + masterIp + ":8020" + twitterPath)
    .filter(line => !line.isEmpty())

  // extract only message from tweets
  val tweets = textFile.flatMap(tweet => {
    parse(tweet) \ "text" match {
      case JString(value) => List(value)
      case _              => List()
    }
  })

  // create RDD[words] from RDD[message]
  val words = tweets.flatMap(text => text.split(" "))

  // cache rdd
  words.cache()

  // calculate total number of words 
  println("Total number of words: " + words.count)

  // calculate for each word total number
  val counts = words.map(word => (word, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)

  // println an order list of words
  counts.saveAsTextFile("hdfs://" + masterIp + ":8020" + "/user/training/output/spark/" + name + "_" + System.currentTimeMillis())
}




