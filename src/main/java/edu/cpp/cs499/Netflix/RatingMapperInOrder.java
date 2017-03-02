package edu.cpp.cs499.Netflix;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingMapperInOrder extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    /**
     * With two instance variables, the map() method is called once per input record, so it pays to avoid unnecessary object creation.
     */
    private DoubleWritable changedKey = new DoubleWritable();
    private Text changedVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] contents = value.toString().split(",");

        changedKey.set(Double.parseDouble(contents[1]));
        changedVal.set((contents[0]));

        context.write(changedKey, changedVal);
    }
}
