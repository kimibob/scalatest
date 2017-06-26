package com.zq.javatest;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\bigdata-sourcecode\\hadoop_src\\hadoop-2.7.0\\");
	    Logger.getRootLogger().setLevel(Level.WARN);
	    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
	    
        String logFile = "d:\\scalatest.txt"; // Should be some file on your system
        JavaSparkContext sc = new JavaSparkContext("local", "Simple App");
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}
