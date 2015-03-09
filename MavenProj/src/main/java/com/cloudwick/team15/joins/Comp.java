package com.cloudwick.team15.joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Comp extends WritableComparator {

	public Comp() {
		super(CustomInput.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CustomInput t1 = (CustomInput) w1;
		CustomInput t2 = (CustomInput) w2;

		int comp = (t1.person).compareTo(t2.person);
		if (comp == 0) {

			if (t1.value > (t2.value))
				comp = -1;
			else if (t1.value > (t2.value))
				comp = 1;
			else
				comp = 0;
		}
		return comp;
	}
}