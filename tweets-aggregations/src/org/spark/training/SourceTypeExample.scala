package org.spark.training

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object SourceTypeExample extends App {

  // create SparkContext
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("source-aggregation-example")
  val sc = new SparkContext(sparkConf)
  //  sc.addJar("jarPath")

  // create RDD[String] from file where each string is a line
  val textFile = sc.textFile("hdfs://ip:port/test")
    .filter(line => !line.isEmpty())

  // extract only source from tweets
  val tweetSource = textFile.flatMap(tweet => {
    parse(tweet) \ "source" match {
      case JString(value) => List(value)
      case _              => List()
    }
  }).map(tweet => Utils.getTwitterSource(tweet))

  // calculating and ordering tweets number per source type
  val sourcesCounting = tweetSource.map(source => (source, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = true)

  sourcesCounting.foreach(x => println(x))
}