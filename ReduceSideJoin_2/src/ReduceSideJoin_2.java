import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoin_2 {

	/**
	 * Define a pair of strings that are writable. They are serialized in a byte
	 * comparable format.
	 */
	public static class WordPair implements WritableComparable<WordPair> {
		private String word1;
		private String word2;
		private boolean sourceIsS;

		public WordPair() {
		}

		public WordPair(String word1, String word2) {
			this.word1 = word1;
			this.word2 = word2;
			this.sourceIsS = false;
		}

		public void set(String word1, String word2) {
			this.word1 = word1;
			this.word2 = word2;
		}

		public void setSource() {
			this.sourceIsS = true;
		}

		public String getFirst() {
			return this.word1;
		}

		public String getSecond() {
			return this.word2;
		}

		public boolean getSource() {
			return this.sourceIsS;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.word1 = in.readUTF();
			this.word2 = in.readUTF();
			this.sourceIsS = in.readBoolean();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(word1);
			out.writeUTF(word2);
			out.writeBoolean(sourceIsS);
		}

		@Override
		public int hashCode() {
			return word1.hashCode() * 127;
		}

		@Override
		public boolean equals(Object right) {
			if (right instanceof WordPair) {
				WordPair r = (WordPair) right;
				return (r.word1.equals(this.word1) && r.word2
						.equals(this.word2)) && (r.sourceIsS == this.sourceIsS);
			} else {
				return false;
			}
		}

		@Override
		public int compareTo(WordPair o) {
			if (!word1.equals(o.word1)) {
				return word1.compareTo(o.word1);
			} else if (!word2.equals(o.word2)) {
				if (o.getSource()) {
					return 1;
				} else if (this.getSource()) {
					return -1;
				} else {
					return word2.compareTo(o.word2);
				}
			} else {
				return 0;
			}
		}
	}

	/**
	 * Partition based on the first part of the pair.
	 */
	public static class FirstPartitioner extends
			Partitioner<WordPair, WordPair> {
		@Override
		public int getPartition(WordPair key, WordPair value, int numPartitions) {
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}

	/**
	 * Compare only the first part of the pair, so that reduce is called once
	 * for each value of the first part.
	 */
	public static class FirstGroupingComparator implements
			RawComparator<WordPair> {
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8,
					b2, s2, Integer.SIZE / 8);
		}

		@Override
		public int compare(WordPair o1, WordPair o2) {
			String l = o1.getFirst();
			String r = o2.getFirst();
			return l.compareTo(r);
		}
	}

	public static class MapClass extends
			Mapper<LongWritable, Text, WordPair, WordPair> {

		private final WordPair key = new WordPair();
		private final WordPair value = new WordPair();

		@Override
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String pathName = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			StringTokenizer itr = new StringTokenizer(inValue.toString());
			String left = new String();
			String right = new String();
			String last = new String();
			if (itr.hasMoreTokens()) {
				left = itr.nextToken();
				if (itr.hasMoreTokens()) {
					right = itr.nextToken();
					if (itr.hasMoreTokens()) {
						last = itr.nextToken();
					}
				}
				key.set(left, right);
				if (pathName.endsWith("databaseS"))
					key.setSource();
				value.set(right, last);
				context.write(key, value);
			}
		}
	}

	public static class Reduce extends Reducer<WordPair, WordPair, Text, Text> {
		private static final Text SEPARATOR = new Text(
				"------------------------------------------------");
		private final Text first = new Text();
		private final Text my_value = new Text();

		@Override
		public void reduce(WordPair key, Iterable<WordPair> values,
				Context context) throws IOException, InterruptedException {
			context.write(SEPARATOR, null);
			for (WordPair value : values) {
				first.set(key.getFirst() + "," + value.getFirst());
				my_value.set(value.getSecond());
				context.write(first, my_value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: secondarysrot <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "secondary sort");
		job.setJarByClass(ReduceSideJoin_2.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		// group and partition by the first string in the pair
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);

		// the map output is WordPair, WordPair
		job.setMapOutputKeyClass(WordPair.class);
		job.setMapOutputValueClass(WordPair.class);

		// the reduce output is Text, Text
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPaths(job, otherArgs[0]);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}