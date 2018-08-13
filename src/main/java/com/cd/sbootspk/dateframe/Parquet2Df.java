package com.cd.sbootspk.dateframe;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;

public class Parquet2Df {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Parquest2Df")
//                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> peopleDF = spark.read().json("projects/sboot-hd/datas/person.json");


        // DataFrames can be saved as Parquet files, maintaining the schema information
        peopleDF.write().parquet("D:\\parquet\\people.parquet");

        // Read in the Parquet file created above.
        // Parquet files are self-describing so the schema is preserved
        // The result of loading a parquet file is also a DataFrame
        Dataset<Row> parquetFileDF = spark.read().parquet("D:\\parquet\\people.parquet");
        parquetFileDF.printSchema();

        // Parquet files can also be used to create a temporary view and then used in SQL statements
        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> personDF = spark.sql("SELECT name, age, tel FROM parquetFile WHERE age BETWEEN 13 AND 19");
        personDF.show();
        Dataset<String> dataset = personDF.map((MapFunction<Row, String>) row -> row.getString(0) + " |\t" + row.getLong(1) + " | " + row.getString(2), Encoders.STRING());
        dataset.show();
        spark.stop();
    }
}
