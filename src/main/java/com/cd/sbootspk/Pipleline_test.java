package com.cd.sbootspk;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Created by DONG on 2018/6/24.
 */
public class Pipleline_test {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("pipleline test").master("local").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//        List<String> strings = Arrays.asList("1", "2");
        JavaRDD<Integer> stringJavaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), 3);
        JavaRDD<Integer> mapRDD = stringJavaRDD.map((Function<Integer, Integer>) s -> {
            System.out.println("map: " + s);
            return s;
        });
        mapRDD.filter(s -> {
            System.out.println("filter: " + s);
            return true;
        }).count();
    }
}
