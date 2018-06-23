package com.cd.sbootspk;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName(WordCount.class.getSimpleName()).master("local").getOrCreate();
        SparkContext sc = spark.sparkContext();
        JavaRDD<String> lines = sc.textFile("projects/sboot-hd/datas/wc.txt", 1).toJavaRDD();
//        JavaRDD<String> lines = spark.read().textFile("projects/sboot-hd/datas/wc.txt").javaRDD();
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(w -> new Tuple2<>(w, 1));
        JavaPairRDD<String, Integer> wordCount = pairRDD.reduceByKey((c1, c2) -> c1 + c2);
        JavaPairRDD<Integer, String> mapToPair = wordCount.mapToPair(t -> new Tuple2<>(t._2, t._1));
        JavaPairRDD<Integer, String> sortedPair = mapToPair.sortByKey(false);
        JavaPairRDD<String, Integer> result = sortedPair.mapToPair(t -> new Tuple2<>(t._2, t._1));
        result.foreach(s -> System.out.println(s));
    }
}
    