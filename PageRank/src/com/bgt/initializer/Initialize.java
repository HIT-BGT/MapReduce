package com.bgt.initializer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
	
public class Initialize {
public static class BuildMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		context.write(new IntWritable(0), new Text("1"));
		System.out.println("0	1");
		String[] result = value.toString().split("\\\t");
		context.write(new IntWritable(Integer.valueOf(result[0])), new Text(result[1]));
		System.out.println(result[0] +"\t" +  result[1]);
	}
}

public static class BuildReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
	double count = 0.0;
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		if (key.get() == 0) {
			for (Text value: values)	count+=Double.valueOf(value.toString());
		}
		else for (Text value: values) context.write(key, new Text(String.valueOf(1.0 / count)+"|"+value.toString()));
	}
}
}
