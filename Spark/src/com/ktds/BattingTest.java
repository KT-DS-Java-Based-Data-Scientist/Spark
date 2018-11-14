package com.ktds;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class BattingTest {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
							.setAppName("Spark Test")
							.setMaster("local[*]");		// spark 서버를 사용할 때 IP 주소 등을 적어줌. (local[*] : 로컬에 있는 모든 코어를 사용해서 분석하라)
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\baseballdatabank-master\\core\\Batting.csv";
		String savePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\baseballdatabank-master\\core\\Batting";
		
		JavaRDD<String> rdd = sc.textFile(filePath);
		
		/*rdd.filter(line -> !line.contains("playerID,yearID,"))
			.map(line -> line.split(","))
			.flatMap(array -> Arrays.stream(array)
									.skip(8)
									.limit(4)
									.map(score -> score.trim())
									.map(score -> array[0] + "," + score)
									.collect(Collectors.toList())
									.iterator()
			)
			.map(score -> score.split(","))
			.mapToPair(scoreArr -> new Tuple2<>(scoreArr[0], Integer.parseInt(scoreArr[1])))
			.reduceByKey((amount, value) -> amount + value)
			.mapToPair(scoreTuple -> new Tuple2<>(scoreTuple._2, scoreTuple._1))
			.repartition(1)
			.sortByKey(false)
			.mapToPair(scoreTuple -> new Tuple2<>(scoreTuple._2, scoreTuple._1))
			.take(3)
			.forEach(tuple -> System.out.println(tuple));
			//.saveAsTextFile(savePath);*/
		
		rdd.filter(line -> !line.contains("playerID,yearID,"))
		.map(line -> line.split(","))
		.flatMapToPair(array -> Arrays.stream(array)
								.skip(8)
								.limit(4)
								.map(score -> score.trim())
								.map(score -> new Tuple2<>(array[0], Integer.parseInt(score)))
								.collect(Collectors.toList())
								.iterator()
		)
		.reduceByKey((amount, value) -> amount + value)
		.repartition(1)
		.mapToPair(scoreTuple -> new Tuple2<>(scoreTuple._2, scoreTuple._1))
		.sortByKey(false)
		.map(tuple -> new Tuple2<>(tuple._2, tuple._1))
		.saveAsTextFile(savePath);
		
		/*sc.parallelize(list)
		  .repartition(1)
		  .saveAsTextFile(savePath);		// take를 사용할 경우 list로 받아와 text파일로 저장할 때*/		
		
		sc.close();				
	}
}
