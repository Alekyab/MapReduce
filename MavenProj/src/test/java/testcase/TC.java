package testcase;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.cloudwick.team15.wc.WordCount;
import com.cloudwick.team15.wc.WCReducer;
import com.cloudwick.team15.wc.WCMapper;

public class TC {

	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		WCMapper mapper = new WCMapper();
		mapDriver = new MapDriver<Object, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		WCReducer reducer = new WCReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(1),
				new Text("orange orange apple"));
		mapDriver.withOutput(new Text("orange"), new IntWritable(1));
		mapDriver.withOutput(new Text("orange"), new IntWritable(1));
		mapDriver.withOutput(new Text("apple"), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("orange"), values);
		reduceDriver.withOutput(new Text("orange"), new IntWritable(2));
		reduceDriver.runTest();
	}

	@Test
	public void testMapperReducer() throws IOException {
		mapReduceDriver.addInput(new LongWritable(1), new Text(
				"orange orange apple"));
		mapReduceDriver.addOutput(new Text("apple"), new IntWritable(1));
		mapReduceDriver.addOutput(new Text("orange"), new IntWritable(2));
		mapReduceDriver.runTest();
	}

}