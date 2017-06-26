package com.zq.scalatest

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SimpleStream {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\bigdata-sourcecode\\hadoop_src\\hadoop-2.7.0\\");
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("10.1.2.149", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}