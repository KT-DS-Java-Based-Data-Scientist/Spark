package com.ktds;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SparkTest {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
							.setAppName("Spark Test")
							.setMaster("local[*]");		// spark 서버를 사용할 때 IP 주소 등을 적어줌. (local[*] : 로컬에 있는 모든 코어를 사용해서 분석하라)
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\text.txt";
		JavaRDD<String> rdd = sc.textFile(filePath);
		rdd/*.filter((word) -> !word.equalsIgnoreCase("cat"))
		   .map(word -> word.toUpperCase())
		   .map(word -> word.replace(".", "")
			       			.replace("&", "")
			)
			.foreach(word -> {							// java 8 이상
				System.out.println(word);
			})*/
		   /*.map(word -> word + " (" + word.length() + ")")
		   .foreach(word -> {							
				System.out.println(word);
			});*/
		   .mapToPair(word -> new Tuple2<>(word, 1))
		   .reduceByKey((amount, value) -> amount + value)
		   .foreach((tuple) -> System.out.println(tuple));
		
		/*rdd.foreach(new VoidFunction<String>() {		// java 1.7
			
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});*/
		
		sc.close();				// 메모리 누수를 막기 위해
	}
}
