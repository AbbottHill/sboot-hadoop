package com.cd.sbootspk.dateframe;

import com.cd.Beans.Person;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * row RDD == DataFrame
 */
public class RDD2DataFrame {
    private static SparkContext sc = SparkSession.builder().appName("RDD 2 DataFrame").master("local").getOrCreate().sparkContext();
    public static JavaSparkContext jsc = new JavaSparkContext(sc);
}

class JSON2RDD2DataFrame {
    public static void main(String[] args) {
        SQLContext sqlContext = new SQLContext(RDD2DataFrame.jsc);
        Dataset<Row> rowRDD = sqlContext.read().json("sboot-hd\\datas\\persion.json");

        rowRDD.show();

        rowRDD.select("name").show();

        rowRDD.select(rowRDD.col("name"), rowRDD.col("age").plus(10).as("年龄")).show();

        rowRDD.filter(rowRDD.col("age").gt(18)).show();

        rowRDD.registerTempTable("pstb");

        sqlContext.sql("select * from pstb where age >= 19").show();

        RDD2DataFrame.jsc.stop();
    }
}

class RDD2DataFrameByReflect {
    public static void main(String[] args) {
        JavaRDD<String> stringJavaRDD = RDD2DataFrame.jsc.textFile("sboot-hd/datas/person.txt");

        JavaRDD<Person> personJavaRDD = stringJavaRDD.map(s -> {
            Person person = new Person();
            String[] strings = s.split(",");
            person.setName(strings[0]);
            person.setAge(Integer.parseInt(strings[1]));
            person.setTel(strings[2]);
            return person;
        });

        SQLContext sqlContext = new SQLContext(RDD2DataFrame.jsc);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(personJavaRDD, Person.class);
        dataFrame.show();

        dataFrame.registerTempTable("persons");
        sqlContext.sql("select name, tel from persons").show();

        RDD2DataFrame.jsc.stop();
    }
}

class RDD2DataFrameByProgram {
    public static void main(String[] args) {
        SQLContext sqlContext = new SQLContext(RDD2DataFrame.jsc);

        JavaRDD<String> stringJavaRDD = RDD2DataFrame.jsc.textFile("sboot-hd/datas/person.txt");

        JavaRDD<Row> rowJavaRDD = stringJavaRDD.map(s -> {
            String[] fields = s.split(",");
            Row row = RowFactory.create(String.valueOf(fields[0]), Integer.valueOf(fields[1]), String.valueOf(fields[2]));
            System.out.println(row.getString(0));
//            GenericRow row = new GenericRow(fields);
            return row;
        });


        StructField[] fields = new StructField[]{DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("tel", DataTypes.StringType, false)};
        StructType schema = new StructType(fields);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowJavaRDD, schema);

        dataFrame.show();
//        dataFrame.foreach(new ForeachFunction<Row>() {
//            @Override
//            public void call(Row row) throws Exception {
//                System.out.println(row.getString(0));
//            }
//        });
        RDD2DataFrame.jsc.stop();
    }
}


