package com.ktds.log.statistics.dao;

import com.ktds.log.statistics.Statistics;

public interface StatisticsDao {

	public int insertStatisticsBySeconds(Statistics statistics);
	
	public int insertStatisticsByMinutes(Statistics statistics);

	public int insertStatisticsByHours(Statistics statistics);
}
