package com.cd.sbootspk;

/* SimpleApp.java */
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SimpleSparkApp {
    public static void main(String[] args) {
//        String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
        System.setProperty("SPARK_HOME", "D:\\Soft\\spark-2.3.1-bin-hadoop2.7");
        String logFile = "D:\\Soft\\spark-2.3.1-bin-hadoop2.7\\README.md";
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("SimpleSparkApp")
//                .config("spark.sql.warehouse.dir", "file:///D:\\Soft\\spark-2.3.1-bin-hadoop2.7")
                .getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(new FilterFunction<String>() {
            @Override
            public boolean call(String s) throws Exception {
                return s.contains("b");
            }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
