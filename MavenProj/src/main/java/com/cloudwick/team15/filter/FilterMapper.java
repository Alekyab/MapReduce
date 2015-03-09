package com.cloudwick.team15.filter;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<Object, Text, Text, Text> {

	// @Override
	@SuppressWarnings("unchecked")
	protected void map(Object offset, Text line, Context context)
			throws IOException, InterruptedException {

		String line1 = line.toString();
		String[] split = line1.split(" ");

		if (split[1].trim().equals("192.168.1.191"))
			context.write(new Text(split[0]), new Text(split[1]));

	}

}
