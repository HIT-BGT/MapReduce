
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class MapSideJoin extends Configured implements Tool{

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		//用于缓存在databaseS中的数据
		private Map<String, String> userMap = new HashMap<String, String>();
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private String[] kv;
		
		public void setup(Context context) throws IOException, InterruptedException{
			BufferedReader in = null;
			try{
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				String sIdNameAge = null;
				for (Path path:paths){
					if (path.toString().endsWith("databaseS")){
						in = new BufferedReader(new FileReader(path.toString()));
						while ((sIdNameAge = in.readLine()) != null){
							StringTokenizer itr = new StringTokenizer(sIdNameAge);
							userMap.put(itr.nextToken(), itr.nextToken()+","+itr.nextToken());
						}
					}
				}
			}catch (IOException e){
				e.printStackTrace();
			}finally{
				try{
					if (in != null){
						in.close();	
					}
				}catch(IOException e){
					e.printStackTrace();
				}
			}
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			kv = value.toString().split(" ");
			if (userMap.containsKey(kv[0])){
				outputKey.set(kv[0]+"\t"+userMap.get(kv[0]));
				outputValue.set(kv[1]+","+kv[2]);
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public static class ReducerClass extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
			Text outputValue = new Text();
			for (Text val : values) {
				outputValue .set(outputValue + val.toString() + "\t");
			}
			context.write(key, outputValue);
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(getConf(), "MapSideJoin");
		job.setJobName("MapSideJoin");
		job.setJarByClass(MapSideJoin.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		//用于缓存在databaseS中的数据
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[2]));
		return job.waitForCompletion(true) ? 0:1;
	}
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(),  new MapSideJoin(), args);
		System.exit(res);
	}
}