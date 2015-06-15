package org.spark.training

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.codehaus.jackson.map.ext.JodaDeserializers.DateTimeDeserializer
import java.util.Calendar

object CreationTimeExample extends App {

  // create SparkContext
  val sparkConf = new SparkConf()
    .setMaster("master")
    .setAppName("creation-time-aggregation-example")
  val sc = new SparkContext(sparkConf)
  sc.addJar("jarPath")

  // create RDD[String] from file where each string is a line
  val textFile = sc.textFile("hdfs://ip:port/test")
    .filter(line => !line.isEmpty())

  // extract only creation time from tweets
  val tweets = textFile.flatMap(tweet => {
    parse(tweet) \ "created_at" match {
      case JString(value) => List(Utils.getTwitterDate(value))
      case _              => List()
    }
  })

  // calculating and ordering tweets number per hour
  val hoursCounting = tweets.map(date => (date.getHourOfDay, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, false)

  hoursCounting.foreach(x => println(x))
}