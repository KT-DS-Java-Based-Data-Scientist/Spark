package com.ktds;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class BattingTest2 {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
							.setAppName("Spark Test")
							.setMaster("local[*]");		// spark 서버를 사용할 때 IP 주소 등을 적어줌. (local[*] : 로컬에 있는 모든 코어를 사용해서 분석하라)
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\baseballdatabank-master\\core\\Batting.csv";
		String savePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\baseballdatabank-master\\core\\Batting2";
		
		JavaRDD<String> rdd = sc.textFile(filePath);
				
		/*rdd.filter(line -> !line.contains("playerID,yearID,"))
			.map(line -> line.split(","))
			.filter(array -> array[1].equals("2017"))
			.flatMapToPair(array -> Arrays.stream(array)
									.skip(8)
									.limit(4)
									.map(score -> score.trim())
									.filter(score -> Integer.parseInt(score) > 0)
									.map(score -> new Tuple2<>(array[0],(Double.parseDouble(score) / Double.parseDouble(array[6]))))
									.collect(Collectors.toList())
									.iterator()
			)
			.reduceByKey((amount, value) -> amount + value)
			.repartition(1)
			.mapToPair(scoreTuple -> new Tuple2<>(scoreTuple._2, scoreTuple._1))
			.sortByKey(false)
			.map(tuple -> new Tuple2<>(tuple._2, tuple._1))
			.foreach(tuple -> System.out.println(tuple));
		//.saveAsTextFile(savePath);*/
		
		rdd.filter(line -> !line.contains("playerID,yearID,"))
			.map(line -> line.split(","))
			.filter(array -> array[1].equals("2017"))
			.filter(array -> !array[6].equals("0"))
			.map(array ->  {
				String[] newArray = new String[6];
				newArray[0] = array[0];
				newArray[1] = array[6];
				newArray[2] = array[8];
				newArray[3] = array[9];
				newArray[4] = array[10];
				newArray[5] = array[11];
				return newArray;
			})
			.map(array -> {
				String[] newArray = new String[3];
				newArray[0] = array[0];
				newArray[1] = array[1];
				
				int sum = Integer.parseInt(array[2]) + Integer.parseInt(array[3]) + Integer.parseInt(array[4]) + Integer.parseInt(array[5]);
				
				newArray[2] = sum + "";
				return newArray;
			})
			.map(array -> {
				String[] newArray = new String[2];
				newArray[0] = array[0];
				
				double hr = Double.parseDouble(array[2]) / Double.parseDouble(array[1]);
				
				newArray[1] = hr + "";
				
				return newArray;
			})
			.flatMapToPair(array -> Arrays.stream(array)
									.skip(1)
									.map(score -> score.trim())
									.map(score -> new Tuple2<>(array[0], Double.parseDouble(score)))
									.collect(Collectors.toList())
									.iterator()
			)
			.reduceByKey((amount, value) -> amount + value)
			.repartition(1)
			.mapToPair(scoreTuple -> new Tuple2<>(scoreTuple._2, scoreTuple._1))
			.sortByKey(false)
			.map(tuple -> new Tuple2<>(tuple._2, (int) (tuple._1 * 1000) / 1000.0))
			.foreach(tuple -> System.out.println(tuple));
		
		sc.close();				
	}
}
