package com.alvin.test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		// 将one变量声明在map函数的外边且声明为static的，节省内存
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				/*
				 * 此处进行一些优化 1.去掉单词首尾的空格 2.去掉单词首尾的标点符号
				 */
				String s = itr.nextToken();
				// 去掉单词首尾的空格
				s = s.trim();
				/*
				 * 用正则表达式去掉一句话中的标点符号、空格和一些能想到的特殊符号，去除符号和空格（中英文）后面一段是中文的 s =
				 * s.replaceAll("(?i)[^a-zA-Z0-9\u4E00-\u9FA5]", "");
				 * 如果这么做，也会把单词中间的标点符号给去掉
				 */
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
				if (!s.isEmpty()) {
					word.set(s);
					context.write(word, one);
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/*
	 * Combiner类规定继承自Reducer
	 */
	public static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/*
	 * Partitioner 自己写的一个例子，算法也是突发奇想，随便写的，可能分类的效果不好
	 */
	public static class MyPartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			// TODO Auto-generated method stub
			/*
			 * 自己随便想的一个hash函数，把一个单词的每个字符的ASCII码的和乘以一个数再跟reducer的数目取余。
			 */
			String skey = key.toString();
			int sum = 0;
			for (int i = 0; i < skey.length(); ++i) {
				sum += skey.charAt(i);
			}
			return sum * 127 % numReduceTasks;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop com.alvin.test.WordCount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(IntSumReducer.class);
		// 因为mapper和reducer输出是同样的类型，setMapOutputKeyClass
		// setMapOutputValueClass可以不调用。
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//设置Partitioner类
		job.setPartitionerClass(MyPartitioner.class);
		// 设置reducer的个数 通过设置reducer的个数为1，经过实验验证了输出结果全写在一个文件中了，结果文件也不大。
		job.setNumReduceTasks(144);
		// 可添加多个输入路径
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		// 设置最后一个参数为输出路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}