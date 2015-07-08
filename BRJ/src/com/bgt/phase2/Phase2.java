package com.bgt.phase2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Phase2 {
	public static class Phase2Mapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			/*Directly output its input*/
			String[] arrvalue = value.toString().split("\\\t");
			context.write(new Text(arrvalue[0]), new Text(arrvalue[1]+"\t"+arrvalue[2]));
		}
	}
	public static class Phase2Reducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			StringBuffer sb = new StringBuffer();
			boolean first = true;
			for (Text value : values){
				if (first){
					sb.append(value.toString()+"\t");
					first = false;
				}else{
					String[] arrvalue = value.toString().split("\\\t");
					sb.append(arrvalue[0]);
				}
			}
			context.write(new Text(sb.toString()), null);
		}
	}
}
