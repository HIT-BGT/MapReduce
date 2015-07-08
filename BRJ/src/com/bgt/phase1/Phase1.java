package com.bgt.phase1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Phase1 {
	public static class Phase1Mapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			FileSplit fs = (FileSplit)context.getInputSplit();
			String fn = fs.getPath().getName();
			if (fn.contains("records")){	//if the input is a record
				String[] arrvalue = value.toString().split("\\|");
				String RID = arrvalue[0];
				context.write(new IntWritable(Integer.valueOf(RID)), value);
				System.out.println(RID+ "|" +value.toString());
			}else if(fn.contains("RIDPairs")){	//if the input is a RID pair
				String[] arrvalue = value.toString().split("\\|");
				String firstRID = arrvalue[0], secondRID = arrvalue[1];
				context.write(new IntWritable(Integer.valueOf(firstRID)), new Text("("+firstRID+","+secondRID+")"+"|"+arrvalue[2]));
				context.write(new IntWritable(Integer.valueOf(secondRID)), new Text("("+firstRID+","+secondRID+")"+"|"+arrvalue[2]));
			}
		}
	}
	public static class Phase1Reducer extends Reducer<IntWritable, Text, Text, Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String record = null;
			List<String> valueList = new ArrayList<String>();
			for (Text value : values){
				valueList.add(value.toString());
			}
			for (int i=0; i<valueList.size(); i++){
				String value = valueList.get(i);
				if (!(value.contains("("))){	//if the value is a record
					record = value;
					break;
				}
			}
			for (int i=0; i<valueList.size(); i++){
				String value = valueList.get(i);
				if (value.toString().contains("(")){	//if the value is a RID pair
					String[] arrvalue = value.split("\\|");
					String RIDPair = arrvalue[0].substring(1, arrvalue[0].length()-1);
					context.write(new Text(RIDPair), new Text(record+"\t"+arrvalue[1]));
				}
			}
		}
	}
}
