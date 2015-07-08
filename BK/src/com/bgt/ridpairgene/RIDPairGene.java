package com.bgt.ridpairgene;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import com.bgt.mycomparator.Compound;
import com.bgt.mycomparator.MyComparator;


public class RIDPairGene {
	
	public static final int prefixLength = 2;
	public static float Similarity(String A1, String A2){
		/*Jaccard Similarity Function*/
		int i; char c;
		float intersection = (float) 0.0, union =(float) A2.length();
		for (i=0; i<A1.length(); i++){
			c = A1.charAt(i);
			if (A2.indexOf(c) >= 0)
				intersection++;
			else 
				union++;
		}
		return (float)(intersection/union);
	};
	public static class RIDPairMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private StringBuffer tokenOrdering = new StringBuffer();
		public void setup(Context context) throws IOException, InterruptedException{
			BufferedReader in = null;
			try{
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				String token = null;
				for (Path path:paths){
						in = new BufferedReader(new FileReader(path.toString()));
						while ((token = in.readLine()) != null){
							tokenOrdering.append(token);
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
			int i=0;
			ArrayList<Compound> complists = new ArrayList<Compound>();
			MyComparator comparator = new MyComparator();
			String[] arrvalue = value.toString().split("\\|");	//extract join-value;
			String[] strvalue = arrvalue[1].split(" ");	//split the join-value according to spaces
			for (i=0; i<strvalue.length; i++) {
				Compound c = new Compound();
				c.token = strvalue[i];
				c.count = tokenOrdering.indexOf(strvalue[i]);
				complists.add(c);
			}
			Collections.sort((List<Compound>)complists, comparator);	//sort the tokens according to their frequencies
			i = 0;
			for (Compound c : complists){	//output the tokens as the sorted sequence
				if (i>=prefixLength) break;
				context.write(new Text(c.token),new Text(arrvalue[0]+","+arrvalue[1]));
				System.out.println(c.token+"|"+arrvalue[0]+","+arrvalue[1]);
				i++;
			}
		}
	}
	public static class RIDPairReducer extends Reducer <Text, Text, Text, Text>{
		private StringBuffer  alreadyOutput= new StringBuffer();
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			ArrayList<Compound> complists = new ArrayList<Compound>();
			for (Text value: values){
				String[] arrvalue = value.toString().split(",");
				Compound c = new Compound();
				c.token = arrvalue[1].replaceAll(" ", "");
				c.count = Integer.valueOf(arrvalue[0]);
				complists.add(c);
			}
			for (int i=0; i<complists.size()-1; i++){
				for (int j=i+1; j<complists.size(); j++){
					float sim = Similarity(complists.get(i).token, complists.get(j).token);
					if (sim >= 0.5 ){ //0.5 is similarity threshold
						String RIDPair = String.valueOf(complists.get(i).count)+","+String.valueOf(complists.get(j).count);
						if (alreadyOutput.indexOf(RIDPair) < 0){
							context.write(new Text(RIDPair.replaceAll(",","|")+"|"+String.valueOf(sim)), new Text());
							alreadyOutput.append(RIDPair);
						}
					}
				}
			}
		}
	}
}
