package com.cd.sbootspk;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by DONG on 2018/6/24.
 */
public class Repartition_test {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("repartition test").master("local").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(
                1, 2, 3, 4, 5, 6, 7, 8, 9
        ), 3);

        JavaRDD<String> stringJavaRDD = javaRDD.mapPartitionsWithIndex((index, iterator) -> {
            ArrayList<String> strings = new ArrayList<>();
            while (iterator.hasNext()) {
                Integer value = iterator.next();
                String sb = new StringBuilder("partition index: " + index).append(" num: " + value).toString();
                strings.add(sb);
                System.out.println(Thread.currentThread().getName() + "  " + sb);
            }
            return strings.iterator();
        }, false);

        long count = stringJavaRDD.coalesce(6, false).mapPartitionsWithIndex((index, iterator) -> {
            ArrayList<Object> objects = new ArrayList<>();
            while (iterator.hasNext()) {
                String x = Thread.currentThread().getName() + "  " + index + "-> " + iterator.next();
                System.out.println(x);
                objects.add(x);
            }
            return objects.iterator();
        }, false).count();

        System.out.println("count " + count);
    }
}

