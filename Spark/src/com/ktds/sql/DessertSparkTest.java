package com.ktds.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DessertSparkTest {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
								.setAppName("Dessert")
								.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		String fileName = "C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data\\dessert-menu.csv";
		
		JavaRDD<Dessert> dessert = sc.textFile(fileName)
									 .filter(line -> line.trim().length() > 0)
									 .map(line -> new Dessert(line.split(",")));
		
		// SparkSQL 을 쓰기 위한 Session
		SparkSession session = SparkSession.builder()
										   .appName("DessertSql")
										   .master("local[*]")
										   .getOrCreate();
		
		// session을 이용해 dessert를 DB화
		Dataset<Row> dessertDataFrame = session.createDataFrame(dessert, Dessert.class);
		
		// SQL 수행을 위한 임시 테이블 생성
		dessertDataFrame.createOrReplaceTempView("dessert_table");
		
		// 쿼리
		StringBuffer query = new StringBuffer();
		query.append(" SELECT	COUNT(*) AS NUM_OF_OVER_300KCAL ");
		query.append(" FROM		DESSERT_TABLE                   ");
		query.append(" WHERE	KCAL >= 260                     ");
		
		Dataset<Row> numOfOver300Kcal = session.sql(query.toString());
		
		query = new StringBuffer();
		query.append(" SELECT	COUNT(*) AS CNT, KCAL ");
		query.append(" FROM		DESSERT_TABLE         ");
		query.append(" GROUP	BY KCAL           ");
		query.append(" ORDER	BY CNT DESC       ");		// 실제 sql에서는 불가능
		query.append("             , KCAL ASC     ");
		
		Dataset<Row> groupDessert = session.sql(query.toString());
		
		// 출력
		//dessertDataFrame.show();
		//numOfOver300Kcal.show(30);
		groupDessert.show(30);
		
		session.close();
		sc.close();
		
	}

}
