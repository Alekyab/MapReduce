package com.cloudwick.team15.joins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author alekya
 *
 */
public class Reducerclass extends Reducer<CustomInput, Text, Text, Text> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */

	protected void reduce(CustomInput line, Iterable<Text> value,
			Context context) throws IOException, InterruptedException {

		String sum = "";
		Iterator<Text> itr = value.iterator();

		sum = itr.next().toString();
		while (itr.hasNext()) {
			context.write(new Text(""), new Text(sum + " " + itr.next()));

		}

	}
}