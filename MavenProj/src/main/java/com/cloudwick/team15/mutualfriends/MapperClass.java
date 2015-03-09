package com.cloudwick.team15.mutualfriends;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, CustomInput, Text> {
	@Override
	protected void map(Object offset, Text line, Context context)
			throws IOException, InterruptedException {

		String line1 = line.toString();
		String[] split = line1.split(":");
		String[] split1 = split[1].split(",");

		context.write(new CustomInput(split[0], 0), new Text(split[1] + ","));

		for (int i = 0; i < split1.length; i++)
			for (int j = 0; j < split1.length; j++) {
				context.write(new CustomInput(split1[i], 1),
						new Text(split1[j]));
			}
	}

}
