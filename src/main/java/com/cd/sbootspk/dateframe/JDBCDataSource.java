package com.cd.sbootspk.dateframe;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class JDBCDataSource {
    public static void main(String[] args) {
        SparkContext sparkContext = SparkSession.builder().appName("jdbc data source").master("local").getOrCreate().sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sparkContext);
        SQLContext sqlContext = new SQLContext(jsc);

        // way 1
        HashMap<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://127.0.0.1:3306/database_mine?characterEncoding=utf8&useSSL=true");
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("user", "root");
        options.put("password", "mysqlpass");
        options.put("dbtable", "sys_user");
        Dataset<Row> dataFrame = sqlContext.read().format("jdbc").options(options).load();
        dataFrame.show();

        options.put("", "");
        options.put("dbtable", "sys_user");

        // way 2


    }



}
