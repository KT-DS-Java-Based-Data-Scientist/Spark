package com.ktds.streaming;

import java.io.Serializable;

public class Visit implements Serializable {

	private static final long serialVersionUID = -3741470500083535598L;

	private final String year;
	private final String month;
	private final String date;
	private final String hour;
	private final String minute;
	private final String second;

	private final String ip;
	private final String url;
	private final String mthd;
	
	public Visit(String[] args) {
		this.year = args[0];
		this.month = args[1];
		this.date = args[2];
		this.hour = args[3];
		this.minute = args[4];
		this.second = args[5];
		this.ip = args[6];
		this.url = args[7];
		this.mthd = args[8];
	}

	public Visit(String year, String month, String date, String hour, String minute, String second, String ip,
			String url, String mthd) {
		this.year = year;
		this.month = month;
		this.date = date;
		this.hour = hour;
		this.minute = minute;
		this.second = second;
		this.ip = ip;
		this.url = url;
		this.mthd = mthd;
	}

	public String getYear() {
		return year;
	}

	public String getMonth() {
		return month;
	}

	public String getDate() {
		return date;
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

	public String getIp() {
		return ip;
	}

	public String getUrl() {
		return url;
	}

	public String getMthd() {
		return mthd;
	}

}
