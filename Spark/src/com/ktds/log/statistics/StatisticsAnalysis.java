package com.ktds.log.statistics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.ktds.log.statistics.dao.StatisticsDao;

public class StatisticsAnalysis {
	
	private static AbstractXmlApplicationContext ctx;		
	
	private static StatisticsDao getDao() {
		if (ctx == null) {
			ctx = new ClassPathXmlApplicationContext("classpath:config/rootContext.xml", "classpath:config/applicationContext.xml");
		}
		return ctx.getBean("statisticsDao", StatisticsDao.class);
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
							.setAppName("Statistics")
							.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = "C:\\logs\\statistics.log";
		
		// Statistics : DateTime : 09:44:38.204, URL : /board/list, IP : 0:0:0:0:0:0:0:1, List Size : 10
		// --> {09, 44, 38, /board/list, 0:0:0:0:0:0:0:1}
		JavaRDD<Statistics> statistics = sc.textFile(filePath)
											.map(line -> {
												String[] resultArray = new String[5];
												
												String[] array = line.split(",");
												
												String[] dateArray = array[0].split(":");
												resultArray[0] = dateArray[2].trim();
												resultArray[1] = dateArray[3].trim();
												resultArray[2] = dateArray[4].split("\\.")[0].trim();
												
												resultArray[3] = array[1].split(":")[1].trim();
												resultArray[4] = array[2].replace("IP : ", "").trim();
												
												return new Statistics(resultArray);
											});
		
		SparkSession session = SparkSession.builder()
											.appName("Statistics SQL")
											.master("local[*]")
											.getOrCreate();

		// 13시에 들어온 요청수
		/*session.createDataFrame(statistics, Statistics.class)
			   .where("hour == 13")
			   .groupBy("hour", "ip")
			   .agg(functions.count("*"))
			   .show();*/
		
		// 가장 많은 요청이 들어온 시간 hour
		/*Dataset<Row> agg = session.createDataFrame(statistics, Statistics.class)
								  .groupBy("hour")
								  .agg(functions.count("*").as("NUM"));
		
		agg.orderBy(agg.col("NUM").desc())
		   .show();*/
					
		// 가장 많은 요청이 들어온 분 minute
		/*Dataset<Row> agg = session.createDataFrame(statistics, Statistics.class)
								  .groupBy("minute")
								  .agg(functions.count("*").as("COUNT"));
		
		agg.orderBy(agg.col("COUNT").desc())
		   .show();*/
		
		session.createDataFrame(statistics, Statistics.class)
			  .groupBy("hour", "minute", "second", "url", "ip")
			  .agg(functions.count("*").as("req_count"))
			  .toJavaRDD()						// foreach를 쓰면 row를 필요로 한다. 우리는 col이 필요해. 그래서 rdd로 바꿔서 
			  .map(row -> {							// 그 값을 불러옴
				  String hour = row.getAs("hour");
				  String minute = row.getAs("minute");
				  String second = row.getAs("second");
				  String url = row.getAs("url");
				  String ip = row.getAs("ip");
				  int req_count = Math.toIntExact(row.getAs("req_count"));		// 이렇게 받아온 값을 statistics에 넣어줌
				  
				  return new Statistics(hour, minute, second, url, ip, req_count);
			  })
			  .foreach(stcs -> StatisticsAnalysis.getDao().insertStatisticsBySeconds(stcs));
			  ;
		
		session.createDataFrame(statistics, Statistics.class)
			  .groupBy("hour", "minute", "url", "ip")
			  .agg(functions.count("*").as("req_count"))
			  .toJavaRDD()						
			  .map(row -> {						
				  String hour = row.getAs("hour");
				  String minute = row.getAs("minute");
				  String url = row.getAs("url");
				  String ip = row.getAs("ip");
				  int req_count = Math.toIntExact(row.getAs("req_count"));		
				  
				  return new Statistics(hour, minute, url, ip, req_count);
			  })
			  .foreach(stcs -> StatisticsAnalysis.getDao().insertStatisticsByMinutes(stcs));
			  ;	  
			 
		session.createDataFrame(statistics, Statistics.class)
			  .groupBy("hour", "url", "ip")
			  .agg(functions.count("*").as("req_count"))
			  .toJavaRDD()						
			  .map(row -> {							
				  String hour = row.getAs("hour");
				  String url = row.getAs("url");
				  String ip = row.getAs("ip");
				  int req_count = Math.toIntExact(row.getAs("req_count"));		
				  
				  return new Statistics(hour, url, ip, req_count);
			  })
			  .foreach(stcs -> StatisticsAnalysis.getDao().insertStatisticsByHours(stcs));
			  ;

		session.close();
		sc.close();
	}
	
}
