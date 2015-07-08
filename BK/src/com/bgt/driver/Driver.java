package com.bgt.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bgt.ridpairgene.RIDPairGene.RIDPairMapper;
import com.bgt.ridpairgene.RIDPairGene.RIDPairReducer;

import org.apache.hadoop.io.Text;

public class Driver extends Configured implements Tool{
	public int run(String args[]) throws Exception{
		Job job = new Job(getConf(), "BK");
		job.setJarByClass(Driver.class);
		
		job.setMapperClass(RIDPairMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(RIDPairReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		//the first argument should be the position of the ordered token file
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());	
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));	
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[2]));	
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(),  new Driver(), args);
		System.exit(res);
	} 
}
