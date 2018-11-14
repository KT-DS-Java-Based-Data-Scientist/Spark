package com.ktds.streaming.dao;

import java.io.Serializable;

import org.mybatis.spring.support.SqlSessionDaoSupport;

import com.ktds.streaming.Visit;

public class VisitStatisticsDaoImpl extends SqlSessionDaoSupport implements VisitStatisticsDao, Serializable {

	private static final long serialVersionUID = -953290278243809494L;

	@Override
	public int insertStatisticsBySeconds(Visit visit) {
		return getSqlSession().insert("VisitStatisticsDao.insertStatisticsBySeconds", visit);
	}

	@Override
	public int insertStatisticsByMinutes(Visit visit) {
		return getSqlSession().insert("VisitStatisticsDao.insertStatisticsByMinutes", visit);
	}

	@Override
	public int insertStatisticsByHours(Visit visit) {
		return getSqlSession().insert("VisitStatisticsDao.insertStatisticsByHours", visit);
	}

	@Override
	public int insertStatisticsByYears(Visit visit) {
		return getSqlSession().insert("VisitStatisticsDao.insertStatisticsByYears", visit);
	}

	@Override
	public int insertStatisticsByMonths(Visit visit) {
		return getSqlSession().insert("VisitStatisticsDao.insertStatisticsByMonths", visit);
	}

	@Override
	public int insertStatisticsByDates(Visit visit) {
		return getSqlSession().insert("VisitStatisticsDao.insertStatisticsByDates", visit);
	}

}
