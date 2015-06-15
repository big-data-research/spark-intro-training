package org.spark.training

import java.text.SimpleDateFormat
import org.joda.time.format.DateTimeFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.jsoup.Jsoup

object Utils extends Serializable {
  def getTwitterDate(date: String) = {
    val twitterFormat = "EEE MMM dd HH:mm:ss Z yyyy"
    val formatter = DateTimeFormat.forPattern(twitterFormat)
    formatter.parseDateTime(date)
  }

  def getTwitterSource(html: String) = {
    Jsoup.parse(html).select("a").text()
  }
}