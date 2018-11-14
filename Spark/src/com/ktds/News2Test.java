package com.ktds;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class News2Test {

	public static void main(String[] args) throws IOException {

		List<String> newsLines = Files.readAllLines(Paths.get("C:\\Users\\YEAH\\Desktop\\ktds\\수업\\data", "News2.txt"));

		/*List<String> newsWord = newsLines.stream()
										 .filter(line -> line.length() > 0) 
										 .flatMap(line -> Arrays.stream(line.split(" ")))
										 .filter(line -> line.length() > 0)
										 .map(word -> word.trim())
										 .collect(Collectors.toList());
		
		for(int i=0; i<newsWord.size()-2; i++) {
			System.out.println(newsWord.get(i) + " " + newsWord.get(i+1) + " " + newsWord.get(i+2));
		}*/
		
		// Stream
		/*newsLines.stream()
				 .filter(line -> line.length() > 0) 
				 .map(line -> line.split(" "))
				 .filter(line -> line.length() > 0)
				 .map(word -> word.trim())
				 .forEach(word -> {
					 for(int i=0; i<word.length-2; i++) {
						 System.out.println(word[i] + " " + word[i+1] + " " + word[i+2]);
					 }
				 });*/
		
		/*newsLines.stream()
				 .filter(line -> line.length() > 0) 
				 .map(line -> Arrays.stream(line.split("[^a-zA-Z0-9가-힣]+"))
						 			.filter(line -> line.length() > 0)
						 			.map(word -> word.trim())
						 			.collect(Collectors.toList())
				 )
				 .flatMap(list -> {
					 List<String> arrList = new ArrayList<>();
					 String word;
					 for (int i=0; i<list.size()-2; i++) {
						 word = list.get(i) + " " + list.get(i+1) + " " + list.get(i+2);
						 arrList.add(word);
					 }
					 return arrList;
				 })
				 .forEach(System.out::println);*/

	}
}
