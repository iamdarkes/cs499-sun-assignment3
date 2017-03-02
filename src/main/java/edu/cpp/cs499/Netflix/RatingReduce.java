package edu.cpp.cs499.Netflix;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce the movies so that each key (movieID) has a value that is the average of the ratings for it
 */
public class RatingReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text text, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        double rating = 0.0;
        int ratingCount = 0;
        String line;
        String movieId = "";

        Iterator<Text> i = values.iterator();
        //iterate through all of the elements
        while (i.hasNext()) {
            line = i.next().toString();
            StringTokenizer st = new StringTokenizer(line, ",");
            //read the rating value
            rating += Double.parseDouble(st.nextToken());
            //keep adding up the ratings
            ratingCount++;

        }
        //get the average rating for each movie
        rating = rating / ratingCount;

        String contents = "";

        contents += movieId + ",";
        contents += rating;

        context.write(text, new Text(contents));
    }

}