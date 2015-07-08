package com.bgt.check;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CheckMethod {
	public static boolean check(String path) throws IOException{
		String uri =path;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = fs.open(new Path(uri));
		DataInputStream datain = new DataInputStream(in);
		@SuppressWarnings("deprecation")
		String temp = datain.readLine();
		String[] result = temp.split("\\\t");
		if (Integer.valueOf(result[1]) == 0){
			return false;
		}else{
			return true;
		}
	}
}
