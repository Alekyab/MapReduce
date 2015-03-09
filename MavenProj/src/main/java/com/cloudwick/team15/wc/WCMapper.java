package com.cloudwick.team15.wc;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	protected void map(Object offset, Text line, Context context)
			throws IOException, InterruptedException {

		String line1 = line.toString();
		String[] split = line1.split(" ");
		for (String splitpart : split) {
			context.write(new Text(splitpart), new IntWritable(1));
			System.out.println(context.getWorkingDirectory());
		}
	}

}
