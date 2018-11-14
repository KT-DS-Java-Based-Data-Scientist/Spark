package com.ktds.streaming;

import java.util.Arrays;

public class SplitTest {

	public static void main(String[] args) {
		
		String data ="#A#B#C#D";
		
		String[] dataArray = data.split("#");
		
		Arrays.stream(dataArray)
			  .forEach(System.out::println);
	}
}
