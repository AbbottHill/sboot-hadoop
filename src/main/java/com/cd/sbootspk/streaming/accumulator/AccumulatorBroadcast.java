package com.cd.sbootspk.streaming.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class AccumulatorBroadcast {

    public static void main(String[] args) throws InterruptedException {
// Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

        // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("cluster01", 9999);

        // Split each line into words
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        /**
         * These variables are copied to each machine, and no updates to the variables on the remote machine are propagated back to the driver program.
         */
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        wordCounts.foreachRDD((rdd, time) -> {
            // Get or register the blacklist Broadcast
            Broadcast<List<String>> blacklist = JavaWordBlacklist.getInstance(new JavaSparkContext(rdd.context()));
            // Get or register the droppedWordsCounter Accumulator
            LongAccumulator droppedWordsCounter = JavaDroppedWordsCounter.getInstance(new JavaSparkContext(rdd.context()));
            // Use blacklist to drop words and use droppedWordsCounter to count them
            String counts = rdd.filter(wordCount -> {
                if (blacklist.value().contains(wordCount._1())) {
                    droppedWordsCounter.add(wordCount._2());
                    return false;
                } else {
                    return true;
                }
            }).collect().toString();
            Date date = new Date(time.milliseconds());
            String output = "Counts at time " + simpleDateFormat.format(date) + " " + counts;
            System.out.println(output);
        });

//        wordCounts.print();
        jssc.start();// Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}

class JavaWordBlacklist {

    private static volatile Broadcast<List<String>> instance = null;

    public static Broadcast<List<String>> getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaWordBlacklist.class) {
                if (instance == null) {
                    List<String> wordBlacklist = Arrays.asList("a", "b", "c");
                    instance = jsc.broadcast(wordBlacklist);
                }
            }
        }
        return instance;
    }
}

class JavaDroppedWordsCounter {

    private static volatile LongAccumulator instance = null;

    public static LongAccumulator getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaDroppedWordsCounter.class) {
                if (instance == null) {
                    instance = jsc.sc().longAccumulator("WordsInBlacklistCounter");
                }
            }
        }
        return instance;
    }
}