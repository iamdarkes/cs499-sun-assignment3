package edu.cpp.cs499.Netflix;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Sorts movies by the average of ratings
 */
public class RatingsDoubleSort extends WritableComparator {

    protected RatingsDoubleSort() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable firstValue, WritableComparable secondValue) {
        DoubleWritable firstDouble = (DoubleWritable) firstValue;
        DoubleWritable secondDouble = (DoubleWritable) secondValue;

        return -1 * firstDouble.compareTo(secondDouble);
    }
}