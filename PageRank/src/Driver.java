import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bgt.initializer.Initialize.BuildMapper;
import com.bgt.initializer.Initialize.BuildReducer;
import com.bgt.pagerank.PageRank.RankMapper;
import com.bgt.pagerank.PageRank.RankReducer;
public class Driver {
	public final static int ITERATION_NUM = 2;
	private static String basePath = "/home/baigt/workspace/PageRank/IO/";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Job buildJob = startBuildJob(basePath+"input", basePath+"initializedInput");
		buildJob.waitForCompletion(true);
		for (int i = 1; i< ITERATION_NUM+1;i++){
			Job rankJob = startRankJob(getInputPath(i), getOutputPath(i));
			rankJob.waitForCompletion(true);
		}
	}
	
	public static Job startBuildJob(String inputPath, String outputPath) throws IOException{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "build");
		job.setJarByClass(Driver.class);
		job.setMapperClass(BuildMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(BuildReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
	
	public static Job startRankJob(String inputPath, String outputPath) throws IOException{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "rank");
		job.setJarByClass(Driver.class);
		job.setMapperClass(RankMapper.class);
		job.setReducerClass(RankReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
	public static String getInputPath(int iteration){
		if (iteration == 1) return basePath+"initializedInput/part-r-00000";
		return basePath+"output" + String.valueOf(iteration-1);
	}
	public static String getOutputPath(int iteration){
		return basePath + "output" + String.valueOf(iteration);
	}
}
