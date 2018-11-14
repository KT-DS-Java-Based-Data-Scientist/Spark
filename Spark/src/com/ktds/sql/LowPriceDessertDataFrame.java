package com.ktds.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class LowPriceDessertDataFrame {
	
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
		
		
		/*dessertDf.select("name", "price")
				 .show(30);*/
		
		dessertDf.select(dessertDf.col("name"), dessertDf.col("price").as("PRICE"))		// data의 변형이 필요할 땐 dataframe.col(컬럼이름)
//				 .where("price >= 4000")
//				 .where("price < 5000")
				 .where(dessertDf.col("price")
						 		 .divide(1000)
						 		 .cast(DataTypes.IntegerType)
						 		 .multiply(1000)
						 		 .$eq$eq$eq(4000))		// ===
				 //.orderBy("price")						// 오름차순
				 .orderBy(dessertDf.col("price").desc()) 	// 내림차순
				 .show(30);
		
		session.close();
		sc.close();
	}

}
