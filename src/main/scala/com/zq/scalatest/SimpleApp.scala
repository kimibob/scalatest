package com.zq.scalatest

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SimpleApp {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\bigdata-sourcecode\\hadoop_src\\hadoop-2.7.0\\")
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val logFile = "d:\\scalatest.txt" // Should be some file on your system  
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}