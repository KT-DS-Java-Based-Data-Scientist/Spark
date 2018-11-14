package com.ktds.log.statistics.dao;

import org.mybatis.spring.support.SqlSessionDaoSupport;

import com.ktds.log.statistics.Statistics;

public class StatisticsDaoImpl extends SqlSessionDaoSupport implements StatisticsDao {

	@Override
	public int insertStatisticsBySeconds(Statistics statistics) {
		return getSqlSession().insert("StatisticsDao.insertStatisticsBySeconds", statistics);
	}

	@Override
	public int insertStatisticsByMinutes(Statistics statistics) {
		return getSqlSession().insert("StatisticsDao.insertStatisticsByMinutes", statistics);
	}

	@Override
	public int insertStatisticsByHours(Statistics statistics) {
		return getSqlSession().insert("StatisticsDao.insertStatisticsByHours", statistics);
	}

}
