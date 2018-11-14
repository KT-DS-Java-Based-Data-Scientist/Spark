package com.ktds;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class contensTest {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
							.setAppName("Spark Test")
							.setMaster("local[*]");		
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\10K.ID.CONTENTS";
		String savePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\10K.ID";
		JavaRDD<String> rdd = sc.textFile(filePath);
		
		rdd.map(data -> data.split("\r\n"))
			.flatMapToPair(array -> Arrays.stream(array)
								.map(line -> line.split("\t"))
								.filter(arr -> arr.length > 1)
								.map(arr -> new Tuple2<>(arr[0], arr[1].length()))
								.collect(Collectors.toList())
								.iterator()
			)
			.repartition(1)
			.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
			.sortByKey(false)
			.saveAsTextFile(savePath);
		
		sc.close();			
	}
}
