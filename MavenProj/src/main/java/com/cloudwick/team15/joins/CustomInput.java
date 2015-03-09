package com.cloudwick.team15.joins;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.TextInputFormat;

public class CustomInput implements WritableComparable<CustomInput> {

	public String person;
	public int value;
	
	public CustomInput()
	{
		
	}
	
	public CustomInput(String name, int value)
	{
	this.person=name;
	this.value=value;
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		 
		person = WritableUtils.readString(in);
		value = WritableUtils.readVInt(in);
	}
		 
		
		public void write(DataOutput out) throws IOException {
		 
		WritableUtils.writeString(out, person);
		WritableUtils.writeVInt(out, value);
		}
		 
		
		public int compareTo(CustomInput o) {
		 
		int result = person.compareTo(o.person);
		if (0 == result) {
		if(value>((CustomInput) o).value)
			result=-1;
		else if(value<((CustomInput) o).value)
		result=1;
		else
			result=0;
		}
		return result;
		}
		
		@Override
		public String toString()
		{
			return person;
		}
}
