package com.cloudwick.team15.uniqueusers;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 */

/**
 * @author alekya
 *
 */

public class UUMapper extends Mapper<Object, Text, Text, Text> {

	// @Override
	@SuppressWarnings("unchecked")
	protected void map(Object offset, Text line, Context context)
			throws IOException, InterruptedException {

		String line1 = line.toString();
		String[] split = line1.split(" ");
		context.write(new Text(split[0]), new Text(split[1]));

	}

}
