package com.ktds.streaming;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.ktds.streaming.dao.VisitStatisticsDao;

public class RealTimeLogAnalysis {
	
	private static AbstractXmlApplicationContext ctx;		
	
	private static VisitStatisticsDao getDao() {
		if (ctx == null) {
			ctx = new ClassPathXmlApplicationContext("classpath:config/rootContext.xml", "classpath:config/applicationContext.xml");
		}
		return ctx.getBean("visitStatisticsDao", VisitStatisticsDao.class);
	}
	
	public static void main(String[] args) {
		
		VisitStatisticsDao dao = RealTimeLogAnalysis.getDao();
		
		SparkConf conf = new SparkConf()
							.setAppName("RealTimeLogAnalysis")
							.setMaster("local[*]");			// 현재 로컬의 모든 cpu를 사용하겠다
		
		JavaSparkContext sc = new JavaSparkContext(conf);	// 정적 파일을 분석할 때 많이 쓰인다.(실시간 X)
		
		// Spark Stream 전용 객체 생성
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));		// kafka의 어떤 topic으로부터 데이터를 받아
		
		// Kafka 연결
		Map<String, Object> kafkaConf = new HashMap<>();
		kafkaConf.put("group.id", "kafka-consumer-group");
		kafkaConf.put("bootstrap.servers", "localhost:9092");		// 여러개 적을 수 있다.
		kafkaConf.put("value.deserializer", StringDeserializer.class);		// Deserializer : serialize id를 찾는 (직렬화 시킨 데이터를 찾아와)
		kafkaConf.put("key.deserializer", StringDeserializer.class);		// 우리가 보낸 것이 String이라 StringDeserializer
																			// 객체를 보낼 수도 있는데 그 때에는 객체Deserializer 사용
		// Topic 정의
		List<String> topic = new ArrayList<>();					// 여러개의 topic을 받아올 수 있다
		topic.add("logTopic");
		
		// Kafka 데이터 받아오기
		JavaInputDStream<ConsumerRecord<String, String>> kafkaDataStream = 			// 실시간 데이터를 받아오는 데이터 타입
				KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topic, kafkaConf));
		
		// #YYYY:MM:DD:HH:MM:SS#127.0.0.1#/HelloSpring/member/login#GET
		kafkaDataStream.map(record -> record.value())		// record : ConsumerRecords
						.filter(log -> log.startsWith("#"))
						.map(log -> log.split("#"))
						.map(logArray -> Arrays.stream(logArray)
												.skip(1)
												.collect(Collectors.toList())
												.toArray(new String[4]))
						.map(logArray -> {
							String[] dateTimeArray = logArray[0].split(":");
							
							List<String> dataList = new ArrayList<>();
							dataList.add(dateTimeArray[0]);		// YYYY
							dataList.add(dateTimeArray[1]);		// MM
							dataList.add(dateTimeArray[2]);		// DD
							dataList.add(dateTimeArray[3]);		// HH
							dataList.add(dateTimeArray[4]);		// MM
							dataList.add(dateTimeArray[5]);		// SS
							dataList.add(logArray[1]);		// IP
							dataList.add(logArray[2]);		// URL
							dataList.add(logArray[3]);		// METHOD
							
							//return new Visit(dataList.toArray(new String[9]));
							return dataList.stream()
											.collect(Collectors.joining("ł"));
						})																// 여기까진 rdd가 아닌 consumerrecord
						.foreachRDD(rdd -> {
							rdd.foreach(log -> {
								KafkaSender.send("SPARK " + log);
							});
						});
						/*.dstream()
						.saveAsTextFiles("C:\\sparklog\\spark-log", "");*/
						//.print();
		
		ssc.start();
		
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			ssc.close();
			e.printStackTrace();
		}
		
	}
	
}
