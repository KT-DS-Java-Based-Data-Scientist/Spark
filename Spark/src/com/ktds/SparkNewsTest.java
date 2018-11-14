package com.ktds;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkNewsTest {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
							.setAppName("Spark Test")
							.setMaster("local[*]");		
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\News.txt";
		String savePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\News_Count";
		JavaRDD<String> rdd = sc.textFile(filePath);

		rdd.filter((line) -> line.trim().length() > 0)
		   .flatMap((line) -> Arrays.asList(line.split(" ")).iterator())
		   .mapToPair((word) -> new Tuple2<>(word, 1))
		   .reduceByKey((amount, value) -> amount + value)
		   .mapToPair((tuple) -> new Tuple2<>(tuple._2, tuple._1))		// 정렬을 위해 word와 갯수를 바꿔줌 (갯수가 key가 되고 word가 value가 됨)
		   .repartition(1)						// 여러개의 파티션으로 나뉜 것을 하나로 합쳐줌 (이거 쓰지 않으면 2개 나옴)
		   .sortByKey(false)					// 내림차순 정렬 (true -> default값, 오름차순)
		   .saveAsTextFile(savePath);			// 결과 파일로 저장
		   /*.foreach((tuple) -> System.out.println(tuple));*/
		
		sc.close();				
	}
}
