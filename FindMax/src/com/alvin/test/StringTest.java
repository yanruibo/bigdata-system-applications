package com.alvin.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StringTest {

	public static void main(String[] args) throws IOException {

		String[] sa = {"@#","123","456"};

		List<String> sList = Arrays.asList(sa);
		Set<String> uniqueWords = new HashSet<String>();

		for (String s : sList) {

			// 过滤
			// 去掉单词首尾的空格
			s = s.trim().toLowerCase();
			// 去除单词首部的标点符号
			s = s.replaceFirst("(?i)^[^a-zA-Z-]+", "");
			/*
			 * 下面是去除单词尾部的标点符号，先把字符串反转，然后再调用replaceFirst，
			 * 去除反转后的字符串首部的标点符号，再把字符串反转过来
			 */
			s = new StringBuilder(s).reverse().toString();
			s = s.replaceFirst("(?i)^[^a-zA-Z-]+", "");
			s = new StringBuilder(s).reverse().toString();
			// 过滤之后s的值可能为空，比如： '91. '34.' 这两个字符串经过前面过滤之后均为空。
			System.out.println(s);
			if(!s.isEmpty()){
				uniqueWords.add(s);
			}
		}
		System.out.println(uniqueWords.size());
	}

}
