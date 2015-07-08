package com.bgt.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bgt.phase1.Phase1.Phase1Mapper;
import com.bgt.phase1.Phase1.Phase1Reducer;
import com.bgt.phase2.Phase2.Phase2Mapper;
import com.bgt.phase2.Phase2.Phase2Reducer;

public class Driver {
	private static String basePath = "/home/baigt/workspace/BRJ/IO/";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Job phase1Job = startPhase1Job(basePath+"input", basePath+"phase1output"); 
		phase1Job.waitForCompletion(true);
		Job phase2Job = startPhase2Job(basePath+"phase1output/part-r-00000", basePath+"phase2Output");
		phase2Job.waitForCompletion(true);
	}
	public static Job startPhase1Job(String inputPath, String outputPath) throws IOException{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "phase1");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Phase1Mapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(Phase1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
	public static Job startPhase2Job(String inputPath, String outputPath) throws IOException{
		Configuration conf =  new Configuration();
		Job job = new Job(conf, "phase2");
		job.setMapperClass(Phase2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(Phase2Reducer.class);
		job.setNumReduceTasks(1);	//Exactly 1 reduce task
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
	
}

