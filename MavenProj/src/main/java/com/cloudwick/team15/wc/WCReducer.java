package com.cloudwick.team15.wc;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author alekya
 *
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text line, Iterable<IntWritable> value,
			Context context) throws IOException, InterruptedException {

		int sum = 0;
		Iterator<IntWritable> itr = value.iterator();

		while (itr.hasNext()) {
			sum = sum + (itr.next()).get();
		}
		context.write(line, new IntWritable(sum));
	}
}