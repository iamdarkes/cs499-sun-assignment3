package edu.cpp.cs499.Netflix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

    public static void main(String[] args) {

        try {
            int exitcode = ToolRunner.run(new Driver(), args);

            int[] topTen = topTen(args[2] + "/part-r-00000");
            HashMap<Integer, String> movieTitles = stringTitles(args[5]);

            System.out.println("\nTop 10 Netflix Movies\n");
            for (int i = 0; i < topTen.length; i++) {
                System.out.println((i + 1) + ") " + movieTitles.get(topTen[i]));
            }

            topTen = topTen(args[4] + "/part-r-00000");
            System.out.println("\nTop 10 Netflix Users\n");
            for (int i = 0; i < topTen.length; i++) {
                System.out.println((i + 1) + ") " + topTen[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Runs all the MapReduce jobs. 4 Jobs Total. An initial MapReduce for ratings and most
     * active users (2) and a secondary mapper to sort (2)
     * @param args command line arguments
     * @return 0
     * @throws Exception
     */
    public int run(String[] args) throws Exception {

        Job job1 = new Job();
        job1.setJarByClass(Driver.class);
        job1.setJobName("Netflix Top Movies");
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setMapperClass(RatingMap.class);
        job1.setReducerClass(RatingReduce.class);
        int job1Complete = job1.waitForCompletion(true) ? 0 : 1;

        if (job1.isSuccessful()) {
            System.out.println("Job 1 Complete");

            Job job2= new Job();
            job2.setJarByClass(Driver.class);
            job2.setJobName("Netflix Top Movies Sorted");
            FileInputFormat.addInputPath(job2, new Path(args[1] + "/part-r-00000"));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            job2.setSortComparatorClass(RatingsDoubleSort.class);
            job2.setOutputKeyClass(DoubleWritable.class);
            job2.setOutputValueClass(Text.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.setMapperClass(RatingMapperInOrder.class);
            int job2Complete = job2.waitForCompletion(true) ? 0 : 1;

            if (job2.isSuccessful()) {
                System.out.println("Job 2 Complete");

                Job job3= new Job();
                job3.setJarByClass(Driver.class);
                job3.setJobName("Netflix Top Users");
                FileInputFormat.addInputPath(job3, new Path(args[0]));
                FileOutputFormat.setOutputPath(job3, new Path(args[3]));
                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(Text.class);
                job3.setOutputValueClass(IntWritable.class);
                job3.setOutputFormatClass(TextOutputFormat.class);
                job3.setMapperClass(UserMap.class);
                job3.setReducerClass(UserReduce.class);
                int job3Complete = job3.waitForCompletion(true) ? 0 : 1;

                if (job3.isSuccessful()) {
                    System.out.println("Job 3 Complete.");

                    Job job4 = new Job();
                    job4.setJarByClass(Driver.class);
                    job4.setJobName("Netflix Top Users Sorted");
                    FileInputFormat.addInputPath(job4, new Path(args[3] + "/part-r-00000"));
                    FileOutputFormat.setOutputPath(job4, new Path(args[4]));
                    job4.setSortComparatorClass(UserIntSort.class);
                    job4.setOutputKeyClass(IntWritable.class);
                    job4.setOutputValueClass(IntWritable.class);
                    job4.setOutputFormatClass(TextOutputFormat.class);
                    job4.setMapperClass(UserMapperInOrder.class);
                    int job4Complete = job4.waitForCompletion(true) ? 0 : 1;

                    if (job4.isSuccessful()) {
                        System.out.println("Job 4 Complete. \nAll Finished.");
                    } else if (!job4.isSuccessful()) {
                        System.out.println("Job 4 Failure");
                    }
                } else if (!job3.isSuccessful()) {
                    System.out.println("Job 3 Failure");
                }
            } else if (!job2.isSuccessful()) {
                System.out.println("Job 2 Failure");
            }
        } else if (!job1.isSuccessful()) {
            System.out.println("Job 1 Failure");
        }

        return 0;
    }

    /**
     * Read top ten from the output files
     * @param path output file
     * @return array containing top ten
     */
    private static int[] topTen(String path) {
        BufferedReader br = null;
        int[] topTen = new int[10];

        try {
            br = new BufferedReader(new FileReader(new File(path)));

            int count = 0;
            String line;
            String[] content = null;

            while ((line = br.readLine()) != null) {
                if (count == 10)
                    break;
                content = line.split("\t");
                topTen[count] = Integer.parseInt(content[1]);
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return topTen;
    }

    /**
     * store movie titles with corresponding movieIDs in hashmap
     * @param path location of movie_titles.txt
     * @return hashmap of movie titles
     */
    private static HashMap<Integer, String> stringTitles(String path) {
        HashMap<Integer, String> movieTitles = null;
        BufferedReader br = null;

        try {
            br = new BufferedReader(new FileReader(new File(path)));

            movieTitles = new HashMap<Integer, String>();

            String line;
            while ((line = br.readLine()) != null) {
                String[] title = line.split(",", 3);
                movieTitles.put(Integer.parseInt(title[0]), title[2]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return movieTitles;
    }
}