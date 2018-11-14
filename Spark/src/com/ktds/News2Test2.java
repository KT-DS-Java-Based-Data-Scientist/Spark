package com.ktds;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.ArrayList;
import scala.Tuple2;
import java.util.stream.Collectors;

public class News2Test2 {

	public static void main(String[] args){
		//RDD
		SparkConf conf = new SparkConf()
				.setAppName("Spark Test")
				.setMaster("local[*]");		// spark 서버를 사용할 때 IP 주소 등을 적어줌. (local[*] : 로컬에 있는 모든 코어를 사용해서 분석하라)

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\News2.txt";
		
		JavaRDD<String> rdd = sc.textFile(filePath);
		
		rdd.map(line -> line.split("\t"))
	        .filter(array -> array.length > 1)
	        .map(array -> array[1].split("[^a-zA-Z0-9]+"))
	        .map(array -> {
	           List<String> wordTrigram = new ArrayList<>();
	           String word;
	           for ( int i = 0; i < array.length; i++) {
	              word = array[i] + " ";
	              if ( (i+2) <= array.length - 1 ) {
	                 word += array[i+1] + " ";
	                 word += array[i+2];
	                 wordTrigram.add(word);
	              }
	           }
	           return wordTrigram;
	        })
	        .flatMapToPair(list -> list.parallelStream()
	                             .filter(word -> word.length() > 0)
	                             .filter(word -> word.split(" ").length == 3)
	                             .map(word -> new Tuple2<>(word.toLowerCase(), 1))
	                             .collect(Collectors.toList())
	                             .iterator())
	        .reduceByKey((amount, count) -> amount + count)
	        .filter(tuple -> tuple._2 > 100)
	        .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
	        .repartition(1)
	        .sortByKey(false)
	        .map(tuple -> new Tuple2<>(tuple._2, tuple._1))
	        .foreach(tuple -> System.out.println(tuple));

	}
}
