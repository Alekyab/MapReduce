package com.cloudwick.team15.mutualfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class CPartitioner extends Partitioner<CustomInput, Text> {
	@Override
	public int getPartition(CustomInput key, Text value, int numReduceTasks) {

		Text newKey = new Text(key.toString());
		HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<Text, Text>();
		try {
			return hashPartitioner.getPartition(newKey, value, numReduceTasks);
		} catch (Exception e) {
			e.printStackTrace();
			return (int) (Math.random() * numReduceTasks); // this would return
															// a random value in
															// the range
					}

	}

}
