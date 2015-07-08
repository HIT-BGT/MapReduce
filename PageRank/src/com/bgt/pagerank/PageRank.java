package com.bgt.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.bgt.graph.Edge;
import com.bgt.graph.Node;

public class PageRank {
	public static class RankMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			Node node = new Node(value.toString());
			double edgeNum = (double) node.getEdges().size();
			if (edgeNum > 0){
				for (Edge edge: node.getEdges()){
					context.write(new IntWritable(edge.getEndPoint()), new Text(String.valueOf(node.getRank()/edgeNum)));
				}
			}
			context.write(new IntWritable(node.getID()), new Text(node.getInfo()));
		}
	}
	public static class RankReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Node node = new Node(key.get());
			for (Text value: values){
				Node node2 = new Node(String.valueOf(key) + "\t" + value.toString());
				if (node2.getEdges().size() > 0) node.setEdges(node2.getEdges());
				else node.setRank(node.getRank() + node2.getRank());
			}
			context.write(key, new Text(node.getInfo()));
		}
	}
}
