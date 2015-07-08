package com.bgt.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bgt.tokenordering.TokenOrdering.TokenCombiner;
import com.bgt.tokenordering.TokenOrdering.TokenMapper;
import com.bgt.tokenordering.TokenOrdering.TokenReducer;


public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		String basePath = "/home/baigt/workspace/OPTO/IO/";
		Job OPTOjob = getOPTOjob(basePath+"input", basePath+"output");
		OPTOjob.waitForCompletion(true);
	}
	public static Job getOPTOjob(String inputPath, String outputPath) throws IOException{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "OPTO");
		job.setJarByClass(Driver.class);
		job.setMapperClass(TokenMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(TokenCombiner.class);
		job.setReducerClass(TokenReducer.class);
		job.setNumReduceTasks(1);	//exactly 1 reduce task
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
}
