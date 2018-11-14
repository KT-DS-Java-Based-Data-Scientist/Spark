package com.ktds.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class LowPriceDessertDataFrameGroupBy {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
								.setAppName("Low Price Dessert")
								.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		String fileName = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\dessert-menu.csv";
		
		JavaRDD<Dessert> dessert = sc.textFile(fileName)
									 .filter(line -> line.trim().length() > 0)
									 .map(line -> new Dessert(line.split(",")));
		
		SparkSession session = SparkSession.builder()
										   .appName("LowPriceDessertSql")
										   .master("local[*]")
										   .getOrCreate();
		
		Dataset<Row> dessertDf = session.createDataFrame(dessert, Dessert.class);
		
		/*dessertDf.groupBy("price")
				 .agg(functions.count("price"))
				 .show(30);*/
		
		// 오름차순
		/*dessertDf.groupBy(dessertDf.col("price")
								   .$div(1000)
								   .cast("int")
								   .$times(1000)
								   .as("PRICE_RANGE"))
				 .agg(functions.count("price").as("PRICE_COUNT"))
				 .orderBy("PRICE_RANGE")
				 .show(30);*/
		
		// 내림차순
		Dataset<Row> aggResult = dessertDf.groupBy(dessertDf.col("price")
														    .$div(1000)
														    .cast("int")
														    .$times(1000)
														    .as("PRICE_RANGE"))
										  .agg(functions.count("price").as("PRICE_COUNT"));
		
		aggResult.orderBy(aggResult.col("PRICE_RANGE").desc())
				 .show(30);
		
		session.close();
		sc.close();
	}

}
