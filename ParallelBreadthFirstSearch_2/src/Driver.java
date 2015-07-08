import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

import com.bgt.search.BreadthFirstSearch.MY_COUNTER;
import com.bgt.search.BreadthFirstSearch.SearchMapper;
import com.bgt.search.BreadthFirstSearch.SearchReducer;

public class Driver {
	
	private static String basePath = "/home/baigt/workspace/ParallelBreadthFirstSearch_2/IO/";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		int counter = 1;
		int flag;
		do{
			Job searchJob = startSearchJob(getSearchInputPath(counter), getSearchOutputPaths(counter) );
			searchJob.waitForCompletion(true);
			Counters counters = searchJob.getCounters();
			Counter my_counter = counters.findCounter(MY_COUNTER.COVERING_NODE_COUNTER);
			flag = (int) my_counter.getValue();
			counter++;
		}while(flag!=0);
		System.out.println("Job Finished!!!");
	}

	public static String getSearchInputPath(int counter) {
		if (counter == 1)
			return basePath+"input";
		else
			return basePath+"output" + String.valueOf(counter - 1);
	}

	public static String getSearchOutputPaths(int counter) {
		return basePath+"output" + String.valueOf(counter);
	}

	public static Job startSearchJob(String inputPath, String outputPath)
			throws IOException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "search");
		job.setJarByClass(Driver.class);
		job.setMapperClass(SearchMapper.class);
		job.setReducerClass(SearchReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
}
