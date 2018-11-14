package com.ktds.log.statistics;

import java.io.Serializable;

public class Statistics implements Serializable {

	private static final long serialVersionUID = -137600048599447155L;

	private final String ip;
	
	private final String url;
	
	private final String hour;
	private final String minute;
	private final String second;
	
	private final int reqCount;
	
	public Statistics(String hour, String url,  String ip, int reqCount) {			// 시간 단위
		this.hour = hour;
		this.minute = "00";
		this.second = "00";
		this.url = url;
		this.ip = ip;
		this.reqCount = reqCount;
	}
	
	public Statistics(String hour, String minute, String url,  String ip, int reqCount) {		// 분 단위
		this.hour = hour;
		this.minute = minute;
		this.second = "00";
		this.url = url;
		this.ip = ip;
		this.reqCount = reqCount;
	}
	
	public Statistics(String hour, String minute, String second, String url,  String ip, int reqCount) {
		this.hour = hour;
		this.minute = minute;
		this.second = second;
		this.url = url;
		this.ip = ip;
		this.reqCount = reqCount;
	}

	public Statistics(String[] args) {
		hour = args[0];
		minute = args[1];
		second = args[2];
		url = args[3];
		ip = args[4];
		reqCount = 0;
	}

	public String getIp() {
		return ip;
	}

	public String getUrl() {
		return url;
	}

	public String getHour() {
		return hour;
	}

	public String getMinute() {
		return minute;
	}

	public String getSecond() {
		return second;
	}

	public int getReqCount() {
		return reqCount;
	}
	
}
