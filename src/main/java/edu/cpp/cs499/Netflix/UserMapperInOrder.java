package edu.cpp.cs499.Netflix;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Final map to display results in correct order
 */
public class UserMapperInOrder extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable changeKey = new IntWritable();
    private IntWritable changedVal = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] contents = value.toString().split("\t");

        changeKey.set(Integer.parseInt(contents[1]));
        changedVal.set(Integer.parseInt(contents[0]));

        context.write(changeKey, changedVal);
    }
}