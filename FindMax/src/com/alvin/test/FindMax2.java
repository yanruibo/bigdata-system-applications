package com.alvin.test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*

export HADOOP_CLASSPATH=FindMax.jar

hadoop com.alvin.test.FindMax /tmp/bigdata/2015/english_novel/* /user/2015210978/finalwork-output
hadoop com.alvin.test.FindMax /usr/yanruibo/A-Game-of-Thrones/* /user/2015210978/finalwork-output

*/

public class FindMax2 {

	public static class FirstMapper extends Mapper<Object, Text, Text, Text> {

		private Text keyText = new Text();
		private Text valueText = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// 获得输入文件名称
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			// System.out.println(fileName);
			String line = value.toString();
			String newLine = line.replaceAll("[^a-zA-Z-]", " ");
			StringTokenizer itr = new StringTokenizer(newLine);

			while (itr.hasMoreTokens()) {

				String s = itr.nextToken();
				// 全部小写化并去掉单词首尾的空格
				
				s = s.toLowerCase().trim();

				if (!s.isEmpty()) {
					keyText.set(fileName);
					valueText.set(s);
					context.write(keyText, valueText);
				}
			}
		}
	}

	public static class FirstReducer extends Reducer<Text, Text, Text, IntWritable> {

		private Text keyValue = new Text();
		private IntWritable max = new IntWritable(0);

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> uniqueWords = new HashSet<String>();
			for (Text val : values) {
				uniqueWords.add(val.toString());
			}

			int setSize = uniqueWords.size();
			System.out.println(key.toString() + " : " + setSize);
			if (max.get() < setSize) {
				keyValue.set(key);;
				max.set(setSize);
			}
			context.write(key, new IntWritable(setSize));
		}

		@Override
		protected void cleanup(Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(keyValue, max);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop com.alvin.test.WordCount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "job");

		job.setJarByClass(FindMax2.class);
		job.setMapperClass(FirstMapper.class);

		job.setReducerClass(FirstReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置reducer的个数 通过设置reducer的个数为1，经过实验验证了输出结果全写在一个文件中了，结果文件也不大。
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
