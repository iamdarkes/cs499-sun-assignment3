package edu.cpp.cs499.Netflix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Sorts users by the amount of ratings
 */
public class UserIntSort extends WritableComparator {

    protected UserIntSort() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable firstVal, WritableComparable secondVal) {
        IntWritable firstInteger = (IntWritable) firstVal;
        IntWritable secondInteger = (IntWritable) secondVal;

        return -1 * firstInteger.compareTo(secondInteger);
    }
}
