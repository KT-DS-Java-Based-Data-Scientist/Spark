package com.ktds;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class ScoreTest {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
							.setAppName("Spark Test")
							.setMaster("local[*]");		// spark 서버를 사용할 때 IP 주소 등을 적어줌. (local[*] : 로컬에 있는 모든 코어를 사용해서 분석하라)
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\성적2.txt";
		String savePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\성적";
		
		JavaRDD<String> rdd = sc.textFile(filePath);
		
		// Title 건너띄기
		/*rdd.filter(line -> !line.contains("Java,Database,"))
		   .mapToPair(line -> new Tuple2<>(line.split(",")[0], line.replace(line.split(",")[0]+",", "")))
		   .foreach((line) -> System.out.println(line));*/
		
		rdd.filter(line -> !line.contains("Java,Database,"))
			.map(line -> line.split(","))
			.flatMap(array -> Arrays.stream(array)
									.skip(1)
									.map(score -> score.trim())
									.map(score -> array[0] + "," + score)
									.collect(Collectors.toList())
									.iterator()
			)
			.map(score -> score.split(","))
			.mapToPair(scoreArr -> new Tuple2<>(scoreArr[0], Integer.parseInt(scoreArr[1])))
			.reduceByKey((amount, value) -> amount + value)
			.mapToPair(scoreTuple -> new Tuple2<>(scoreTuple._2 / 7, scoreTuple._1))
			.repartition(1)
			.sortByKey(false)
			.foreach(tuple -> System.out.println(tuple));
		
		/*rdd.filter(line -> !line.contains("Java,Database,"))
		   .map(line -> line.split(","))
		   .mapToPair(array -> {
			   String name = array[0];
			   int average = (int)Arrays.stream(array)
					   			   .skip(1)
					   			   .mapToInt(score -> Integer.parseInt(score.trim()))		// boxing, 기본형....
					   			   .average()
					   			   .orElse(0);		// nullpointexception을 나오지 않도록 하기 위해 optional이라는 것을 만듬 (1.8) 
			   										// -> null일수도 값이 있을 수도
			   return new Tuple2<>(average, name);
		   })
		   .repartition(1)
		   .sortByKey(false)
		   .map(tuple -> new Tuple2<>(tuple._2, tuple._1))
		   .foreach(tuple -> System.out.println(tuple));*/
		
		sc.close();				
	}
}
