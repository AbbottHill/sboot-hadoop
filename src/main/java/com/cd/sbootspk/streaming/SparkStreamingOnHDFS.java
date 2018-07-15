package com.cd.sbootspk.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

// driver height available
public class SparkStreamingOnHDFS {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("spark stream on hdfs");
        final String checkpointDir = "hdfs://cluster01:9000/spark/stream";
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));


    }

}
