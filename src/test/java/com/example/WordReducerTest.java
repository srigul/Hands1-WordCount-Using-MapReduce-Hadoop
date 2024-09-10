package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class WordReducerTest {

    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        WordReducer reducer = new WordReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("Hadoop"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
                    .withOutput(new Text("Hadoop"), new IntWritable(2))
                    .runTest();
    }
}
