package com.cd.sbootspk;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Created by DONG on 2018/6/24.
 */
public class BroadcastVariable_test {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("broadcast variable").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(0, 2, 99, 9, 7, 6));
        int criterion = 6;
//        parallelize.filter(i -> i > criterion).foreach(s -> System.out.println(s));
        Broadcast<Integer> critical = jsc.broadcast(criterion);
        parallelize.filter(i -> i > critical.value()).foreach(s -> System.out.println(s));
        jsc.stop();
    }
}

class Blacklist {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("blacklist filter").master("local").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Broadcast<List<String>> blaclist = jsc.broadcast(Arrays.asList("12345", "1234", "1821"));

        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList(
                "123", "1234", "1821",
                "123456", "1234", "18256",
                "114", "1821", "189"
        ));
        rdd.filter(s -> !blaclist.value().contains(s)).foreach(s -> System.out.println(s));
        jsc.stop();
    }
}


