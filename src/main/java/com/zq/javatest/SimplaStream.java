package com.zq.javatest;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;


public class SimplaStream {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\bigdata-sourcecode\\hadoop_src\\hadoop-2.7.0\\");
	    Logger.getRootLogger().setLevel(Level.WARN);
	    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
	    
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(1));
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream(
				"10.1.2.149", 9999);

		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterator<String> call(String x) {
						return Arrays.asList(x.split(" ")).iterator();
					}
				});
		JavaPairDStream<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s, 1);
					}
				});
		JavaPairDStream<String, Integer> wordCounts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});
		wordCounts.print();
		jssc.start();         
		try {
			jssc.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		
	}
}
