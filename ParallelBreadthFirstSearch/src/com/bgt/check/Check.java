package com.bgt.check;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.bgt.graph.Node;


public class Check {
	public static class CheckMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			Node node = new Node(value.toString());
			if (node.getStatus() == 1) context.write(new IntWritable(1), new IntWritable(1));
			else context.write(new IntWritable(1), new IntWritable(0));
		}
	}
	
	public static class CheckReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		private int sum = 0;
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			for (IntWritable value: values){
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
