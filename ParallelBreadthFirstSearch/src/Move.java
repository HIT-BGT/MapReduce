import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.bgt.check.Check.CheckMapper;
import com.bgt.check.Check.CheckReducer;
import com.bgt.check.CheckMethod;
import com.bgt.search.BreadthFirstSearch.SearchMapper;
import com.bgt.search.BreadthFirstSearch.SearchReducer;

public class Move {
	
	private static String basePath = "/home/baigt/workspace/ParallelBreadthFirstSearch/IO/";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		int counter = 1;
		String checkPath;
		do{
			Job searchJob = startSearchJob(getSearchInputPath(counter), getElsePaths(counter) );
			searchJob.waitForCompletion(true);
			Job checkJob = startCheckJob(getElsePaths(counter), getCheckOutPath(counter));
			checkJob.waitForCompletion(true);
			checkPath = getCheckOutPath(counter)  + "/part-r-00000";
			counter++;
		}while(CheckMethod.check(checkPath));
		System.out.println("Job Finished!!!");
	}

	public static String getSearchInputPath(int counter) {
		if (counter == 1)
			return basePath+"input";
		else
			return basePath+"output" + String.valueOf(counter - 1);
	}

	public static String getElsePaths(int counter) {
		return basePath+"output" + String.valueOf(counter);
	}

	public static String getCheckOutPath(int counter){
		return basePath+"check" + String.valueOf(counter); 
	}

	public static Job startSearchJob(String inputPath, String outputPath)
			throws IOException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "search");
		job.setJarByClass(Move.class);
		job.setMapperClass(SearchMapper.class);
		job.setReducerClass(SearchReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
	
	public static Job startCheckJob(String inputPath, String outputPath) throws IOException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "check");
		job.setJarByClass(Move.class);
		job.setMapperClass(CheckMapper.class);
		job.setReducerClass(CheckReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}

}
