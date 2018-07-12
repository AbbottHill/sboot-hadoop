package com.cd.sbootspk;

import com.cd.Beans.SecondarySortKey;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SecondarySort {
    public static void main(String[] args) {
        SparkContext sparkContext = SparkSession.builder().master("local").appName("secondary sort").getOrCreate().sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sparkContext);

        List<String> strings = Arrays.asList("1 1", "1 0", "2 9", "2 5");
        JavaRDD<String> javaRDD = jsc.parallelize(strings);

        JavaPairRDD<SecondarySortKey, Object> pairRDD = javaRDD.mapToPair(s -> {
            SecondarySortKey ss = new SecondarySortKey();
            String[] words = s.split(" ");
            ss.setFirstColumn(words[0]);
            ss.setSecondColumn(words[1]);
            Tuple2<SecondarySortKey, Object> tuple2 = new Tuple2<>(ss, s);
            return tuple2;
        });
        pairRDD.sortByKey().foreach(s -> System.out.println(s));

        jsc.stop();
    }
}
