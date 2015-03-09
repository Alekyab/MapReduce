package com.cloudwick.team15.mutualfriends;



import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
 
public class GroupingComp extends WritableComparator {
 
protected GroupingComp() {
 
super(CustomInput.class, true);
}
 
@SuppressWarnings("rawtypes")
@Override
public int compare(WritableComparable w1, WritableComparable w2) {
 
CustomInput key1 = (CustomInput) w1;
CustomInput key2 = (CustomInput) w2;
return key1.person.compareTo(key2.person);
}
}