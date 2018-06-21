package com.cd.sbootspk;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.cd.sbootspk.Operators.sc;

public class Operators {
    public static SparkConf conf = new SparkConf().
            setAppName(MapOperator.class.getSimpleName())
            .setMaster("local");
    public static JavaSparkContext sc = new JavaSparkContext(conf);
}

class MapOperator {

    public static void main(String[] args) {
        List<String> datas = Arrays.asList(
                "{'id':1,'name':'xl1','pwd':'xl123','sex':2}",
                "{'id':2,'name':'xl2','pwd':'xl123','sex':1}",
                "{'id':3,'name':'xl3','pwd':'xl123','sex':2}");

        JavaRDD<String> dataRDD = sc.parallelize(datas);
        JavaRDD<User> mapRDD = dataRDD.map(
                new Function<String, User>() {
                    @Override
                    public User call(String v) throws Exception {
                        return JSONObject.parseObject(v, User.class);
                    }
                });

        mapRDD.foreach(new VoidFunction<User>() {
            @Override
            public void call(User user) throws Exception {
                System.out.println("id: " + user.id
                        + " name: " + user.name
                        + " pwd: " + user.pwd
                        + " sex:" + user.sex);
            }
        });

        JavaRDD<User> myMapRDD = dataRDD.map(v -> JSONObject.parseObject(v, User.class));
        myMapRDD.foreach(u -> System.out.println(u.id + " - " + u.name));

        sc.close();
    }

    public static class User {
        String id;
        String name;
        String pwd;
        String sex;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPwd() {
            return pwd;
        }

        public void setPwd(String pwd) {
            this.pwd = pwd;
        }

        public String getSex() {
            return sex;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }
    }

}

class FilterOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(FilterOperator.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> javaRDD = sc.parallelize(list);

        JavaRDD<Integer> filterRDD = javaRDD.filter(v -> v > 3);
        filterRDD.foreach(v -> System.out.println(v));
    }
}

class FlatMapOperator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(FlatMapOperator.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<String> datas = Arrays.asList("a,1", "b,2", "c,3");
        JavaRDD<String> javaRDD = sc.parallelize(datas);
        JavaRDD<String> flatMap = javaRDD.flatMap(s -> Arrays.asList(s.split(",")).iterator());
        flatMap.foreach(s -> System.out.println(s));

    }
}


class MapPartitionsOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(MapPartitionsOperator.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> names = Arrays.asList("张三1", "李四1", "王五1", "张三2", "李四2",
                "王五2", "张三3", "李四3", "王五3", "张三4");

        JavaRDD<String> namesRDD = sc.parallelize(names, 3);
        JavaRDD<String> mapPartitionsRDD = namesRDD.mapPartitions(
                new FlatMapFunction<Iterator<String>, String>() {
                    int count = 0;

                    @Override
                    public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
                        List<String> list = new ArrayList<>();
                        while (stringIterator.hasNext()) {
                            list.add("分区内索引:" + count++ + "\t" + stringIterator.next());
                        }
                        return list.iterator();
                    }
                }
        );

        // 从集群获取数据到本地内存中,慎用 collect
        List<String> result = mapPartitionsRDD.collect();
        result.forEach(System.out::println);

        sc.close();
    }
}

class MapPartitionsOperatorWithIndex {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(MapPartitionsOperator.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> names = Arrays.asList("张三1", "李四1", "王五1", "张三2", "李四2",
                "王五2", "张三3", "李四3", "王五3", "张三4");

        // 初始化，分为3个分区
        JavaRDD<String> namesRDD = sc.parallelize(names, 3);
        JavaRDD<String> mapPartitionsWithIndexRDD = namesRDD.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<String>, Iterator<String>>() {
                    private static final long serialVersionUID = 1L;

                    public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
                        List<String> list = new ArrayList<String>();
                        while (v2.hasNext()) {
                            list.add("分区索引:" + v1 + "\t" + v2.next());
                        }
                        return list.iterator();
                    }
                },
                true);

        // 从集群获取数据到本地内存中
        List<String> result = mapPartitionsWithIndexRDD.collect();
        result.forEach(System.out::println);

        sc.close();
    }
}

class GroupByKeyOperator {
    public static void main(String[] args) {
        List<Tuple2<Object, Object>> list = Arrays.asList(new Tuple2<Object, Object>('a', 1),
                new Tuple2<Object, Object>('b', 2),
                new Tuple2<Object, Object>('b', 2.5),
                new Tuple2<Object, Object>('c', 3));
        JavaPairRDD<Object, Object> pairRDD = sc.parallelizePairs(list);
        pairRDD.foreach(v -> System.out.println(v._1 + ": " + v._2));

        pairRDD.groupByKey().foreach((VoidFunction<Tuple2<Object, Iterable<Object>>>) tuple -> {
            StringBuilder stringBuilder = new StringBuilder(tuple._1.toString()).append("-> ");
            Iterator<Object> iterator = tuple._2.iterator();
            while (iterator.hasNext()) {
                stringBuilder.append(" " + iterator.next());
            }
            System.out.println(stringBuilder);
        });
        sc.close();
    }
}

class DistinctOperator {
    public static void main(String[] args) {
        sc.close();
    }
}




    