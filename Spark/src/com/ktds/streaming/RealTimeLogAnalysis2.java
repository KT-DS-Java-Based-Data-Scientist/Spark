package com.ktds.streaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

public class RealTimeLogAnalysis2 {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
							.setAppName("RealTimeLogAnalysis2")
							.setMaster("local[*]");	
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));
		
		Map<String, Object> kafkaConf = new HashMap<>();
		kafkaConf.put("group.id", "kafka-consumer-group");
		kafkaConf.put("bootstrap.servers", "localhost:9092");		
		kafkaConf.put("value.deserializer", StringDeserializer.class);		
		kafkaConf.put("key.deserializer", StringDeserializer.class);
		
		List<String> topic = new ArrayList<>();
		topic.add("SparkTopic");
		
		JavaInputDStream<ConsumerRecord<String, String>> kafkaDataStream = 
				KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topic, kafkaConf));
		
		kafkaDataStream.map(record -> record.value())
						.map(log -> log.split("#"))
						.mapToPair(logArray -> {
							String[] timeArray = logArray[0].split(" ");
							
							String[] dataList = new String[3];
							dataList[0] = timeArray[0];
							dataList[1] = logArray[3];
							dataList[2] = logArray[4];

							return new Tuple2<>(new Tuple3<>(dataList[0], dataList[1], dataList[2]), 1);
						})
						.reduceByKey((amount, value) -> amount + value)
						.map(tuple -> tuple._1._1() + "ł" + tuple._1._2() + "ł" + tuple._1._3() + "ł" + tuple._2())
						.foreachRDD(log -> {
							log.foreach(l-> KafkaSender.send("BatchTopic", l));
						});
		
		kafkaDataStream.map(record -> record.value())
						.map(log -> log.split("#"))
						.filter(logArray -> !logArray[2].equals(""))
						.mapToPair(logArray -> {
							String[] timeArray = logArray[0].split(" ");
							
							return new Tuple2<>(new Tuple5<>(logArray[1], logArray[2], timeArray[0], logArray[3], logArray[4]),1 );
						})
						.reduceByKey((amount, value) -> amount+value)
						.map(tuple -> tuple._1._1() + "ł" + tuple._1._2() + "ł" + tuple._1._3() + "ł" + tuple._1._4() + "ł" + tuple._1._5() + "ł" + tuple._2())
						.foreachRDD(log -> {
							log.foreach(l -> KafkaSender.send("IDTopic", l));
						});
		
		kafkaDataStream.map(record -> record.value())
						.window(Durations.minutes(5))				// 위에서 처리시간이 정해져 있기 때문에 데이터를 갖고 있을 시간만 정해주면 됨. --> 5분간 데이터를 갖고 있겠다
						.map(log -> log.split("#"))
						.mapToPair(logArray -> {
							String[] timeArray = logArray[0].split(":");
							String time = timeArray[0] + ":" + timeArray[1];
							
							return new Tuple2<>(new Tuple2<>(time, logArray[3]),1 );
						})
						.reduceByKey((amount, value) -> amount+value)
						.map(tuple -> tuple._1._1() + "ł" + tuple._1._2() + "ł" + tuple._2())
						.foreachRDD(log -> {
							log.foreach(l -> KafkaSender.send("RealTimeTopic", l));
						});
		
		ssc.start();
		
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			ssc.close();
			e.printStackTrace();
		}
	}
}
