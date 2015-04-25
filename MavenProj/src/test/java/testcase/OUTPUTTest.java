package testcase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.el.Logger;
import org.apache.hadoop.record.RecordInput;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import testcase.PigUDF;

public class OUTPUTTest {
	@Test
	public void testRecordMerger() {

		try {
			ArrayList in = new ArrayList();
			in.add("alekya");
			in.add("reddy");
			ArrayList in2 = new ArrayList();
			in2.add("alekya");
			in2.add("reddyy");
			TupleFactory tf = TupleFactory.getInstance();

			PigUDF func = new PigUDF();
			
			DataBag bg = DefaultBagFactory.getInstance().newDefaultBag();
			bg.add(tf.newTuple(in));
//List output = (List) func.exec(tf.newTuple(in));
			bg.add(tf.newTuple(in2));
			List output = (List) func.exec(tf.newTuple(bg));
			Assert.assertTrue(output.contains("alekya"));
			Assert.assertTrue(output.contains("reddy"));
			Assert.assertTrue(!(output.contains("reddyy")));
			Assert.assertTrue(!(output.size()==2));
		
		} catch (IOException e) {
			Assert.fail();
		}
	}
}
