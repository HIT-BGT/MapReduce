package com.bgt.phase1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Phase1 {
	public static class Phase1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{ 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] arrvalue = value.toString().split("\\|");
			String strvalue = arrvalue[1];
			StringTokenizer itr = new StringTokenizer(strvalue);
			while (itr.hasMoreTokens()){
				context.write(new Text(itr.nextToken()), new IntWritable(1));
			}
		}
	}
	public static class Phase1Reducer extends Reducer<Text, IntWritable, Text, IntWritable >{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int count = 0;
			for (IntWritable value : values){
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
}
