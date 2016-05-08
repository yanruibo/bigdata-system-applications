package com.alvin.test;

import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public final class WordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: WordCount <file>");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("WordCount");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});
		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				/*
				 * 此处进行一些优化 1.去掉单词首尾的空格 2.去掉单词首尾的标点符号
				 */
				// 去掉单词首尾的空格
				s = s.trim();
				// 去除单词首部的标点符号
				s = s.replaceFirst("(?i)^[^a-zA-Z]+", "");
				/*
				 * 下面是去除单词尾部的标点符号，先把字符串反转，然后再调用replaceFirst，
				 * 去除反转后的字符串首部的标点符号，再把字符串反转过来
				 */
				s = new StringBuilder(s).reverse().toString();
				s = s.replaceFirst("(?i)^[^a-zA-Z]+", "");
				s = new StringBuilder(s).reverse().toString();
				// 过滤之后s的值可能为空，比如： '91. '34.' 这两个字符串经过前面过滤之后均为空。
				if (s.isEmpty()) {

					return new Tuple2<String, Integer>("", 0);
				}
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		ones.repartition(50);
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		// counts.saveAsTextFile("hdfs:/user/2015210978/hw2-output1");
		// 调用hdfs接口写文件
		String dest = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dest), conf);
		OutputStream out = fs.create(new Path(dest));
		counts.repartition(50);
		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			//写入hdfs文件中
			out.write(tuple._1().toString().getBytes());
			out.write(' ');
			out.write(tuple._2().toString().getBytes());
			out.write('\n');
		}
		out.flush();
		out.close();
		ctx.stop();
	}
}