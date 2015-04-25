package testcase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class PigUDF  extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple arg0) throws IOException {
		// TODO Auto-generated method stub
		System.out.println(arg0.get(0));
		String x=arg0.get(0).toString().replaceAll("\\{","").replaceAll("\\}", "").replaceAll("\\(", "").replaceAll("\\)","").replaceAll("reddyy", "");
		System.out.println(x);
		List c=Arrays.asList(x.split(","));
		
		return c;
	}

	
	
}
