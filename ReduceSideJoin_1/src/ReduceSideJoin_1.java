 
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoin_1 {

  public static class TokenizerMapper extends
      Mapper<Object, Text, Text, Text> {
    private Text output_key = new Text();
    private Text output_value = new Text();
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
     if (itr.hasMoreTokens()) output_key.set(itr.nextToken());
     if (itr.countTokens()==2) output_value.set(itr.nextToken()+","+itr.nextToken());
      context.write(output_key, output_value);
    }
  }

  public static class ConnectionReducer extends
      Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String sum = new String();
      for (Text val : values) {
        sum = sum+' '+val.toString();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs =
        new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: RelationConnection1 <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "RelationConnection1");
    job.setJarByClass(ReduceSideJoin_1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(ConnectionReducer.class);
    job.setReducerClass(ConnectionReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPaths(job, otherArgs[0]);
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}