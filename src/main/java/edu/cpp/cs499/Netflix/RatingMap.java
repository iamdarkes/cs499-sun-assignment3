package edu.cpp.cs499.Netflix;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMap extends Mapper<LongWritable, Text, Text, Text> {

    //TrainingRatings.txt starts with <movieId, userId, Rating>
    //This will create a file like <movieId Rating>

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        //Convert Text value to string
        String line = value.toString();
        // tokenize the strings on ","
        StringTokenizer st = new StringTokenizer(line, ",");
        //String name to hold the movieID
        String name = st.nextToken();
        //skip userID
        st.nextToken();
        //set the movieID as the Key for the output <K V> pair
        word.set(name);
        //string to hold rating
        String rating = "";
        rating = st.nextToken();

        context.write(word, new Text(rating));
    }
}
