package com.cd.sbootspk.dateframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class JDBCDataSource {
    public static void main(String[] args) {

        // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
        // Loading data from a JDBC source
        SQLContext spark = SparkSession.builder().master("local").getOrCreate().sqlContext();
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3306/database_mine?characterEncoding=utf8&useSSL=true")
                .option("dbtable", "sys_user")
                .option("user", "root")
                .option("password", "mysqlpass")
                .load();
        jdbcDF.show();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "mysqlpass");
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:mysql://127.0.0.1:3306/database_mine?characterEncoding=utf8&useSSL=true", "sys_user", connectionProperties);
        jdbcDF2.show();

//        // todo
//        // Saving data to a JDBC source
//        jdbcDF.write()
//                .format("jdbc")
//                .option("url", "jdbc:postgresql:dbserver")
//                .option("dbtable", "schema.tablename")
//                .option("user", "username")
//                .option("password", "password")
//                .save();
//
//        jdbcDF.write()
//                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
//
//        // Specifying create table column data types on write
//        jdbcDF.write()
//                .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
//                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
    }

}
