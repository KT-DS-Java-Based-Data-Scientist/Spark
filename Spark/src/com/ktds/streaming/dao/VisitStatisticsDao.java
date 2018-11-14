package com.ktds.streaming.dao;

import com.ktds.streaming.Visit;

public interface VisitStatisticsDao {

	public int insertStatisticsByYears(Visit visit);
	
	public int insertStatisticsByMonths(Visit visit);

	public int insertStatisticsByDates(Visit visit);
	
	public int insertStatisticsBySeconds(Visit visit);
	
	public int insertStatisticsByMinutes(Visit visit);

	public int insertStatisticsByHours(Visit visit);
}
