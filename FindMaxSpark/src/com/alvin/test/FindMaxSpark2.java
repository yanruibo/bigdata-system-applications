package com.alvin.test;

import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import breeze.linalg.unique;
import scala.Tuple2;

public final class FindMaxSpark2 {

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("missing parameters!");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("FindMaxSpark");

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaPairRDD<String, String> fileNameAndContent = ctx.wholeTextFiles(args[0]);

		JavaPairRDD<String, Integer> fileNameAndCount = fileNameAndContent
				.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, String> t) throws Exception {
						// TODO Auto-generated method stub

						String newContent = t._2.replaceAll("[^a-zA-Z-]", " ");
						StringTokenizer itr = new StringTokenizer(newContent);
						
						Set<String> uniqueWords = new HashSet<String>();

						while (itr.hasMoreTokens()) {
							/*
							 * 去掉单词首尾的空格并去掉单词首尾的标点符号
							 */
							String s = itr.nextToken();
							// 全部小写化并去掉单词首尾的空格
							s = s.toLowerCase().trim();
							// 过滤之后s的值可能为空，比如： '91. '34.' 这两个字符串经过前面过滤之后均为空。
							if (!s.isEmpty()) {
								uniqueWords.add(s);
							}

						}
						
						return new Tuple2<String, Integer>(t._1, uniqueWords.size());

					}
				});

		class MyComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

			/**
			*
			*/
			private static final long serialVersionUID = 1L;

			@Override
			public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
				// TODO Auto-generated method stub
				return o1._2.compareTo(o2._2);
			}

		}
		Tuple2<String, Integer> maxFileTuple = fileNameAndCount.max(new MyComparator());

		// counts.saveAsTextFile("hdfs:/user/2015210978/hw2-output1");
		// 调用hdfs接口写文件
		String dest = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dest), conf);
		OutputStream out = fs.create(new Path(dest));
		List<Tuple2<String, Integer>> output = fileNameAndCount.collect();
		for (Tuple2<?, ?> tuple : output) {
			// 写入hdfs文件中
			out.write(tuple._1().toString().getBytes());
			out.write(' ');
			out.write(tuple._2().toString().getBytes());
			out.write('\n');
		}
		out.write("max file info:\n".getBytes());
		out.write(maxFileTuple._1().toString().getBytes());
		out.write(' ');
		out.write(maxFileTuple._2().toString().getBytes());
		out.write('\n');

		out.flush();
		out.close();
		ctx.stop();
	}
}