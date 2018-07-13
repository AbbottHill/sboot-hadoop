package com.cd.sbootspk;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

// driver height available
public class SparkStreamingOnHDFS {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[1]").setAppName("spark stream on hdfs");
        final String checkpointDir = "hdfs://cluster01:9000/spark/stream";

        JavaStreamingContext.getOrCreate(checkpointDir, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                return createcontex;
            }
        });
    }

}
