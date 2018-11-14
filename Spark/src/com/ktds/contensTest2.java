package com.ktds;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class contensTest2 {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
							.setAppName("Spark Test")
							.setMaster("local[*]");		
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\10K.ID.CONTENTS";
		String savePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\10K.ID2";
		JavaRDD<String> rdd = sc.textFile(filePath);
		
		rdd.map(data -> data.split("\t"))
			.filter(array -> array.length > 1)
			.map(array -> array[1].split("[^a-zA-Z0-9]+"))
			.flatMapToPair(array -> Arrays.asList(array)
										  .parallelStream()
										  .filter(word -> word.trim().length() > 0)
										  .map(word -> new Tuple2<>(word, 1))
										  .collect(Collectors.toList())
										  .iterator()
			)
			.reduceByKey((amount, value) -> amount+value )
			.filter(tuple -> tuple._2 > 10000)
			.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
			.repartition(1)
			.sortByKey(false)
			.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
			.saveAsTextFile(savePath);
		
		sc.close();			
	}
}
