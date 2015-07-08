package com.bgt.simpairjoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SimPairJoin {
	public static class SimPairJoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Map<String, ArrayList<String>> RIDPairMap = new HashMap<String, ArrayList<String>>();
		public void setup(Context context) throws IOException, InterruptedException{
			BufferedReader in = null;
			try{
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				String RIDPair = null;
				for (Path path:paths){
						in = new BufferedReader(new FileReader(path.toString()));
						while ((RIDPair = in.readLine()) != null){
							String[] arr = RIDPair.split("\\|");
							String idandsim = arr[1]+"|"+arr[2].substring(0, arr[2].length()-1);
							if (!RIDPairMap.containsKey(arr[0])){	//if there is no key as the same as arr[0] in the hashmap
								ArrayList<String> pair = new ArrayList<String>();
								pair.add(idandsim);
								RIDPairMap.put(arr[0], pair);
							}else{	//if hashmap has already got the key arr[0]
								RIDPairMap.get(arr[0]).add(idandsim);
							}
							idandsim = arr[0]+"|"+arr[2].substring(0, arr[2].length()-1);	//quite similar with above, just use arr[1] as key
							if (!RIDPairMap.containsKey(arr[1])){	
								ArrayList<String> pair = new ArrayList<String>();
								pair.add(idandsim);
								RIDPairMap.put(arr[1], pair);
							}else{	
								RIDPairMap.get(arr[1]).add(idandsim);
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
			String[] arrvalues = value.toString().split("\\|");
			String RID = arrvalues[0];
			if (RIDPairMap.containsKey(RID)){	//if this record is similar with others
				ArrayList<String> pair = RIDPairMap.get(RID);
				for (int i=0; i<pair.size(); i++){
					String[] arrpair = pair.get(i).split("\\|");
					if (Integer.valueOf(RID) < Integer.valueOf(arrpair[0])){
						context.write(new Text(RID + "," + arrpair[0]), new Text(value.toString() + "\t" + arrpair[1]));
					}else{
						context.write(new Text(arrpair[0] + "," + RID), new Text(value.toString() + "\t" + arrpair[1]));
					}
				}
			}
		}
	}
	public static class SimPairJoinReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			StringBuffer sb = new StringBuffer();
			boolean first = true;
			for (Text value : values){
				if (first){
					sb.append(value.toString()+"\t");
					first = false;
				}else{
					String[] arrvalue = value.toString().split("\\\t");
					sb.append(arrvalue[0]);
				}
			}
			context.write(new Text(sb.toString()), null);
		}
	}
}
