package com.bgt.phase2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Phase2 {
	public static class Phase2Mapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		private IntWritable outputkey = new IntWritable();
		private Text outputvalue = new Text();
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			/*Swap key and value*/
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()){
				outputvalue.set(itr.nextToken());
				outputkey.set(Integer.parseInt(itr.nextToken()));
				context.write(outputkey, outputvalue);
			}
		}
	}
	public static class Phase2Reducer extends Reducer<IntWritable, Text, Text, Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text value : values){
				context.write(value, null);
			}
		}
	}
}
