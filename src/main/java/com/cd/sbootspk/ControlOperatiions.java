package com.cd.sbootspk;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class ControlOperatiions {

}

/**
 * cache 机制是每计算出一个要 cache 的 partition 就直接将其 cache 到内存了。
 * 但 checkpoint 没有使用这种第一次计算得到就存储的方法，而是等到 job 结束后另外启动专门的 job 去完成 checkpoint 。
 * 也就是说需要 checkpoint 的 RDD 会被计算两次。因此，在使用 rdd.checkpoint() 的时候，
 * 建议加上 rdd.cache()， 这样第二次运行的 job 就不用再去计算该 rdd 了，直接读取 cache 写磁盘。
 */
class Checkpoint_test {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("checkpoint test").master("local").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//        jsc.setCheckpointDir("D:\\data\\spk-checkpoint");
        JavaRDD<String> lines = jsc.textFile("projects/sboot-hd/datas/wc.txt");
//        JavaRDD<String> wordsRDD = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//        JavaPairRDD<String, Integer> wordOnePair = wordsRDD.mapToPair(s -> new Tuple2<>(s, 1));
//        JavaPairRDD<String, Integer> wordCount = wordOnePair.reduceByKey((i1, i2) -> i1 + i2);
//        lines.cache();
//        lines.checkpoint();
        long start1 = System.currentTimeMillis();
//        wordCount.foreach(t -> System.out.println(t._1 + ": " + t._2));
        lines.count();
        long end1 = System.currentTimeMillis();
        System.out.println("time01: " + (end1 - start1));
        long start2 = System.currentTimeMillis();
//        wordCount.foreach(t -> System.out.println(t._1 + ": " + t._2));
        lines.count();
        long end2 = System.currentTimeMillis();

        System.out.println("time02: " + (end2 - start2));
        jsc.stop();
    }
}

