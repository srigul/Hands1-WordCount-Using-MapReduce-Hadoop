package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class WordCountMapReduceTest {

    private static MiniMRYarnCluster miniCluster;
    private static FileSystem fs;
    private static Configuration conf;

    @BeforeClass
    public static void setUp() throws Exception {
        conf = new Configuration();
        miniCluster = new MiniMRYarnCluster("test-cluster");
        miniCluster.init(conf);
        miniCluster.start();
        fs = FileSystem.get(conf);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (miniCluster != null) {
            miniCluster.stop();
        }
    }

    @Test
    public void testMapReduce() throws Exception {
        // Set up input and output paths
        Path inputPath = new Path("/input");
        Path outputPath = new Path("/output");

        // Create input file
        fs.mkdirs(inputPath);
        fs.create(new Path(inputPath, "input.txt")).write("Hadoop is amazing\n".getBytes());

        // Configure and run the job
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCountMapReduceTest.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Run the job and assert completion
        boolean result = job.waitForCompletion(true);
        assert result : "Job failed";

        // Verify output
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(outputPath, "part-r-00000"))));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        reader.close();
    }
}
