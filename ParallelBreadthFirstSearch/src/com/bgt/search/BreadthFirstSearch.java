package com.bgt.search;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.bgt.graph.*;


public class BreadthFirstSearch {
	public static class SearchMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		public  void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			Node node =  new Node(value.toString());
			if (node.getStatus() == 1){
				for (Edge edge : node.getEdges()){
					Node node2 = new Node(edge.getEndPoint());
					node2.setStatus(1);
					node2.setDistance(node.getDistance() + edge.getWeight());
					context.write(new IntWritable(node2.getID()),new Text( node2.getInfo()));
					System.out.println(String.valueOf(node2.getID()) + "\t" + node2.getInfo());
				}
				node.setStatus(2);
			}
			context.write(new IntWritable(node.getID()),new Text(node.getInfo()));
		}
	}
	public static class SearchReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Node node  = new Node(key.get());
			for (Text value : values){
				Node node2 = new Node(String.valueOf(key)+"\t"+value.toString());
				if (node2.getEdges().size() > 0){
					node.setEdges(node2.getEdges());
				}
				if (node.getDistance() > node2.getDistance()){
					node.setDistance(node2.getDistance());
					if (node2.getStatus() == 1) node.setStatus(1);
				}
				if (node.getStatus()<node2.getStatus()){
					node.setStatus(node2.getStatus());
				}
			}
			
			context.write(key,new Text(node.getInfo()));
		}
	}
}
