package com.bgt.tokenordering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import com.bgt.mycomparator.Compound;
import com.bgt.mycomparator.MyComparator;

public class TokenOrdering {
	public static class TokenMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		/*The same Mapper as BTO*/
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] arrvalue = value.toString().split("\\|");
			String strvalue = arrvalue[1];
			StringTokenizer itr = new StringTokenizer(strvalue);
			while (itr.hasMoreTokens()){
				context.write(new Text(itr.nextToken()), new IntWritable(1));
			}
		}
	}
	public static class TokenCombiner extends Reducer<Text, IntWritable, Text, IntWritable >{
		/*The same Combiner as BTO*/
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int count = 0;
			for (IntWritable value : values){
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	public static class TokenReducer extends Reducer<Text, IntWritable, Text, Text>{
		ArrayList<Compound> complists = new ArrayList<Compound>();
		MyComparator comparator = new MyComparator();
		public void reduce(Text key, Iterable<IntWritable> values, Context context){
			int sum = 0;
			for (IntWritable value : values){
				sum += value.get();
			}
			Compound c = new Compound();
			c.token = key.toString();
			c.count = sum;
			complists.add(c);
		}
		public void cleanup(Context context) throws IOException, InterruptedException{
			Collections.sort((List<Compound>)complists, comparator);	//sort the tokens according to their frequencies
			for (Compound c : complists){	//output the tokens as the sorted sequence
				context.write(new Text(c.token), null);
			}
		}
	}
}
