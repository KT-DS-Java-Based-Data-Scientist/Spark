package com.ktds.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LowPriceDessert {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
								.setAppName("Low Price Dessert")
								.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		String fileName = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\dessert-menu.csv";
		
		JavaRDD<Dessert> dessert = sc.textFile(fileName)
									 .filter(line -> line.trim().length() > 0)
									 .map(line -> {
										 String[] array = line.split(",");
										 int kcal = Integer.parseInt(array[3]);
										 kcal = kcal / 100 * 100;
										 array[3] = kcal + "";
										 return new Dessert(array);
									 });
		
		SparkSession session = SparkSession.builder()
										   .appName("LowPriceDessertSql")
										   .master("local[*]")
										   .getOrCreate();
		
		Dataset<Row> dessertDataframe = session.createDataFrame(dessert, Dessert.class);
		
		dessertDataframe.createOrReplaceTempView("low_dessert_table");
		
		StringBuffer query = new StringBuffer();
		
		query.append(" SELECT	KCAL, SUM(PRICE) AS PRICE      ");
		query.append(" FROM	LOW_DESSERT_TABLE  ");
		query.append(" WHERE	PRICE <= 5000  ");
		query.append(" GROUP	BY KCAL ");
		query.append(" ORDER	BY KCAL DESC ");
		
		Dataset<Row> lower5000 = session.sql(query.toString());
		
		lower5000.show();
		
		session.close();
		sc.close();
	}

}
