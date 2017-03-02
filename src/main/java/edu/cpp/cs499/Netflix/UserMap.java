package edu.cpp.cs499.Netflix;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    //TrainingRatings.txt starts with <movieId, userId, Rating>
    //This will create a file like <userID 1>

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        //Convert Text value to string
        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line, ",");

        //Skip the movieID
        st.nextToken();

        //userID
        String userID = st.nextToken();

        //set the userID as the Key for the output <K V> pair
        word.set(userID);

        //skip the rating
        st.nextToken();

        //write <userID 1>
        context.write(word, one);
    }
}
