package com.cloudwick.team15.uniqueusers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * @author alekya
 *
 */
public class UUReducer extends Reducer<Text, Text, Text, IntWritable> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */

	@Override
	protected void reduce(Text line, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		Iterator<Text> itr = value.iterator();
		HashSet<String> hSet = new HashSet<String>();
		while (itr.hasNext()) {
			hSet.add(itr.next().toString());

		}
		context.write(line, new IntWritable(hSet.size()));
	}
}
