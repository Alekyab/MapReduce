package com.cloudwick.team15.mutualfriends;

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
		HashSet<String> arr = new HashSet<String>();
		HashSet<String> arr1 = new HashSet<String>();

		while (itr.hasNext()) {

			sum = itr.next().toString();

			if (sum.contains(",")) {

				String[] x = sum.split(",");
				for (String x1 : x) {
					arr.add(x1);

				}
			}

			else
				arr1.add(sum.toString());

		}

		Iterator itr1 = arr.iterator();
		while (itr1.hasNext()) {
			arr1.remove(itr1.next());
		}
		arr1.remove(line.toString().trim());
		context.write(new Text(line.toString()), new Text(arr1.toString()));
	}
}