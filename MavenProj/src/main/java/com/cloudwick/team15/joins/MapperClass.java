package com.cloudwick.team15.joins;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, CustomInput, Text> {

	// @Override
	protected void map(Object offset, Text line, Context context)
			throws IOException, InterruptedException {

		String line1 = line.toString();
		String[] split = line1.split(" ");

		if (context
				.getInputSplit()
				.toString()
				.contains(
						"file:/home/hadoop/workspace/MavenProj/input/input2/Emp"))

		{
			context.write(new CustomInput(split[2], 0), line);
		} else
			context.write(new CustomInput(split[0], 1), line);
	}

}
