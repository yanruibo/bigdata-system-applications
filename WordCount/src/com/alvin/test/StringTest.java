package com.alvin.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class StringTest {
	
	public static void main(String[] args) throws IOException {
		
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(new FileReader(new File("/home/yanruibo/hw1/part-r-00000")));
		System.setOut(new PrintStream(new FileOutputStream("output.txt")));
		String s = "";
		while((s=reader.readLine()) != null){
			String[] sa = s.split("\t");
			
			System.out.println("before:  "+sa[0]);
			
			s = sa[0].trim().replaceFirst("(?i)^[^a-zA-Z]+", "");
			
			s = new StringBuilder(s).reverse().toString();
			
			s = s.replaceFirst("(?i)^[^a-zA-Z]+", "");
			
			s = new StringBuilder(s).reverse().toString();
			
			
			System.out.println("after :  "+s);
		}
		
		
		
	}

}
