package com.zq.javatest;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.zq.javatest.dao.DbPoolConnection;

/**
 * 
 *Spark Streaming 从Kafka中接收数据，其有两种方法：（1）、使用Receivers和Kafka高层次的API；（2）、使用 Direct API，这是使用低层次的Kafka API，并没有使用到Receivers，是Spark1.3.0中开始引入。

这里使用的是第二种 Direct API 方式，所以对其进行简单的介绍一下：其会定期地从 Kafka 的 topic+partition 中查询最新的偏移量，再根据定义的偏移量范围在每个 batch 里面处理数据。当作业需要处理的数据来临时，spark 通过调用 Kafka 的简单消费者 API 读取一定范围的数据。 
和基于Receiver方式相比，这种方式主要有一些几个优点： 
　　（1）、简化并行。我们不需要创建多个 Kafka 输入流，然后 union 他们。而使用 directStream，Spark Streaming 将会创建和 Kafka 分区一样的 RDD 分区个数，而且会从 Kafka 并行地读取数据，也就是说Spark 分区将会和 Kafka 分区有一一对应的关系，这对我们来说很容易理解和使用； 
　　（2）、高效。第一种实现零数据丢失是通过将数据预先保存在 WAL 中，这将会复制一遍数据，这种方式实际上很不高效，因为这导致了数据被拷贝两次：一次是被 Kafka 复制；另一次是写到 WAL 中。但是 Direct API 方法因为没有 Receiver，从而消除了这个问题，所以不需要 WAL 日志； 
　　（3）、恰好一次语义（Exactly-once semantics）。通过使用 Kafka 高层次的 API 把偏移量写入 Zookeeper 中，这是读取 Kafka 中数据的传统方法。虽然这种方法可以保证零数据丢失，但是还是存在一些情况导致数据会丢失，因为在失败情况下通过 Spark Streaming 读取偏移量和 Zookeeper 中存储的偏移量可能不一致。而 Direct API 方法是通过 Kafka 低层次的 API，并没有使用到 Zookeeper，偏移量仅仅被 Spark Streaming 保存在 Checkpoint 中。这就消除了 Spark Streaming 和 Zookeeper 中偏移量的不一致，而且可以保证每个记录仅仅被 Spark Streaming 读取一次，即使是出现故障。

但是本方法唯一的坏处就是没有更新 Zookeeper 中的偏移量，所以基于 Zookeeper 的 Kafka 监控工具将会无法显示消费的状况。然而你可以通过 Spark 提供的 API 手动地将偏移量写入到 Zookeeper 中。
 * 2017年6月26日 下午4:31:50
 */
public class KafkaOffset {
	
	static final String checkpointDirectory = "d:\\ck";
	static DbPoolConnection dbp = null;
	
	public static void printRDD(JavaRDD<String> rdd){  
        System.out.println("开始打印");  
        rdd.foreach(new VoidFunction<String>() {
			@Override
			public void call(String t) throws Exception {
				System.out.println("value = "+t);  
			}
		});
        System.out.println("结束打印");  
    }  
	
	public static void printPair(JavaPairRDD<String, Integer> pairRdd ){  
		   
        System.out.println("开始打印!!");  
        Map<String, Integer> value = pairRdd.collectAsMap();
        HashMap<String, Integer> hmap = new HashMap<String, Integer>(value);
        Iterator iter = hmap.entrySet().iterator(); 
        while (iter.hasNext()) { 
            Map.Entry entry = (Map.Entry) iter.next(); 
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue()); 
        } 
        System.out.println("结束打印!!");  
    }  
	
	public static void printPairDStream(JavaPairDStream<String, Integer> lineLengths ){  
        lineLengths.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
			@Override
			public void call(JavaPairRDD<String, Integer> t) throws Exception {
				printPair(t);
			}
		});
    }  
	
	public static void printKafkaConsumerOffset(JavaInputDStream<ConsumerRecord<String, String>> kafkaStream, AtomicReference<OffsetRange[]> oR){  
	    final AtomicReference<OffsetRange[]> abc = oR;
		kafkaStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
			  @Override
			  public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
			    final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			    abc.set(offsetRanges);
			    rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
			      @Override
			      public void call(Iterator<ConsumerRecord<String, String>> consumerRecords) {
			        OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
			        System.out.println("==>>"+
			          o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
			      }
			    });
			  }
			});
    }  
	
	public static void storeKafkaConsumerOffset(JavaInputDStream<ConsumerRecord<String, String>> kafkaStream, AtomicReference<OffsetRange[]> oR){
		final AtomicReference<OffsetRange[]> abc = oR;
		kafkaStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String,String>>>() {
			@Override
			public void call(JavaRDD<ConsumerRecord<String, String>> t)
					throws Exception {
					DruidPooledConnection conn = dbp.getConnection();
	                Statement stmt = conn.createStatement();
	                for (OffsetRange offsetRange : abc.get()) {
	                	ResultSet rs = stmt.executeQuery("select offset from kafka_offsets where topic = '"+offsetRange.topic()+"' and partition = '"+offsetRange.partition()+"'");
	                	if(rs.next()){
	                		long lastOffset = Long.parseLong(rs.getString(1));
	                		//if(lastOffset < offsetRange.untilOffset()){
	                			stmt.executeUpdate("update kafka_offsets set offset ='"
			                            + offsetRange.untilOffset() + "'  where topic='"
			                            + offsetRange.topic() + "' and partition='"
			                            + offsetRange.partition() + "'");
	                			System.out.println("~~~update "+offsetRange.topic()+".part_"+offsetRange.partition()+" from "+lastOffset+" to "+offsetRange.untilOffset());
	                		//}
	                	}else{
	                		String sql = "insert into kafka_offsets values('"+offsetRange.topic()+"','"+offsetRange.partition()+"','"+offsetRange.untilOffset()+"')";
	                		stmt.executeUpdate(sql);
	                	}
	                }
	                //conn.commit();
	                stmt.close();
	                conn.close();
//				for (OffsetRange offsetRange : abc.get()) {
//					System.out.println("commit->"+offsetRange.topic()+"--"+offsetRange.partition()+"--"+offsetRange.untilOffset());
//                }
				
			}
		});
    }  
	
	
	public static JavaStreamingContext createContext() {
		
		SparkConf conf = new SparkConf().setMaster("local[3]").setAppName(
				"SparkStreamingOnKafkaDirect");
		JavaStreamingContext jsc = new JavaStreamingContext(conf,
				Durations.seconds(5));
		jsc.checkpoint(checkpointDirectory);
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "192.168.1.101:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spark_kafka");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("kafka_test");

		JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils
				.createDirectStream(jsc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String> Subscribe(topics,
								kafkaParams));
		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
		//打印offset同时设置offset到全局变量offsetRanges中
		printKafkaConsumerOffset(kafkaStream, offsetRanges);
		
		JavaDStream<String> words = kafkaStream
				.flatMap(new FlatMapFunction<ConsumerRecord<String,String>, String>() {

					@Override
					public Iterator<String> call(ConsumerRecord<String, String> record)
							throws Exception {
						//System.out.println("-->>"+record.value());
						return Arrays.asList(record.value().split(" ")).iterator();
					}
				});

		JavaPairDStream<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {

					public Tuple2<String, Integer> call(String word)
							throws Exception {
						return new Tuple2<String, Integer>(word, 1);
					}
				});

		JavaPairDStream<String, Integer> wordsCount = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1 + v2;
					}
				});
		//业务流程处理完后提交失误offset
		storeKafkaConsumerOffset(kafkaStream, offsetRanges);
		//spark driver print default num is 10
		//wordsCount.print(50);
		//print(wordsCount);
		return jsc;
	}

	public static void deleteFile(File file){
		if (file.exists()) {
			if (file.isFile()) {
			  file.delete();
			} else if (file.isDirectory()) {
		      File[] files = file.listFiles();
		      for (int i = 0;i < files.length;i ++) {
		    	  deleteFile(files[i]);
		      }  
		      file.delete();
			} 
		}
	}
	
	public static void main(String[] args) {
		
		//checkpoint 方式有个不好的地方，修改代码重编译后checkpoint的内容就无法反序列化还原了，而且调试更改后的代码还容易不生效(反序列化之前的代码，成功则用之前checkpoint的代码，不成功则抛错)
		deleteFile(new File(checkpointDirectory));
		
		System.setProperty("hadoop.home.dir", "E:\\bigdata-sourcecode\\hadoop_src\\hadoop-2.7.0\\");
	    Logger.getRootLogger().setLevel(Level.WARN);
	    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
	    
		dbp = DbPoolConnection.getInstance();
		
		Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>() {
		    @Override
		    public JavaStreamingContext call() {
		        return createContext();
		    }
		};
		JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(
				checkpointDirectory, createContextFunc);

		jsc.start();
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		jsc.close();
	}
}
