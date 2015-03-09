package com.cloudwick.team15.replicatedjoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, Text, Text> {

	private HashMap<String, String> deptTable = new HashMap<String, String>();

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path cachefile = new Path(
				"/home/hadoop/workspace/MavenProj/input/input1/Dept");
		FileStatus[] list = fs.globStatus(cachefile);
		for (FileStatus status : list) {
			DistributedCache.addCacheFile(status.getPath().toUri(), conf);
		}
		URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
		Path getPath = new Path(cacheFiles[0].getPath());
		BufferedReader bf = new BufferedReader(new InputStreamReader(
				fs.open(getPath)));
		String setupData = null;
		while ((setupData = bf.readLine()) != null) {
			String[] split = setupData.split(" ");
			deptTable.put(split[0], split[1]);
		}
	}

	@Override
	protected void map(Object offset, Text line, Context context)
			throws IOException, InterruptedException {
		String line1 = line.toString();
		String[] split = line1.split(" ");
		if (deptTable.containsKey(split[2])) {
			context.write(new Text(line + " " + deptTable.get(split[2])),
					new Text(""));
		}

	}

}
