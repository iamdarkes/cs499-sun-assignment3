package edu.cpp.cs499.Netflix;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce the users so that each key (userID) has a value that is the amount of ratings they've made
 */
public class UserReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text text, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int sum = 0;
        Iterator<IntWritable> i = values.iterator();

        while (i.hasNext()) {

            int count = i.next().get();
            sum += count;

        }
        context.write(text, new IntWritable(sum));
    }

}