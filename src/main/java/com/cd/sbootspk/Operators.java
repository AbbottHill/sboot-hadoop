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
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.cd.sbootspk.Operators.sc;

/**
 * genericity is important
 */
public class Operators {
    public static SparkConf conf = new SparkConf().
            setAppName(Operators.class.getSimpleName())
            .setMaster("local");
    public static JavaSparkContext sc = new JavaSparkContext(conf);
}

/**
 * todo
 * aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。和aggregate函数类似，aggregateByKey返回值得类型不需要和RDD中value的类型一致。因为aggregateByKey是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是Pair RDD，对应的结果是Key和聚合好的值；而aggregate函数直接返回非RDD的结果。
 *
 * zeroValue：表示在每个分区中第一次拿到key值时,用于创建一个返回类型的函数,这个函数最终会被包装成先生成一个返回类型,然后通过调用seqOp函数,把第一个key对应的value添加到这个类型U的变量中。
 seqOp：这个用于把迭代分区中key对应的值添加到zeroValue创建的U类型实例中。
 combOp：这个用于合并每个分区中聚合过来的两个U类型的值。
 */
class AggregateByKey {
    public static void main(String[] args) {

        List<Tuple2<Integer, Integer>> datas = new ArrayList<>();
        datas.add(new Tuple2<>(1, 3));
        datas.add(new Tuple2<>(1, 2));
        datas.add(new Tuple2<>(1, 4));
        datas.add(new Tuple2<>(2, 3));

        sc.parallelizePairs(datas, 2)
                .aggregateByKey(
                        0,
                        new Function2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer v1, Integer v2) throws Exception {
                                System.out.println("seq: " + v1 + "\t" + v2);
                                return Math.max(v1, v2);
                            }
                        },
                        new Function2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer v1, Integer v2) throws Exception {
                                System.out.println("comb: " + v1 + "\t" + v2);
                                return v1 + v2;
                            }
                        })
                .collect()
                .forEach(System.out::println);


//        seq: 0	3
//        seq: 3	2
//        seq: 0	4
//        seq: 0	3
//        comb: 3	4
//        (2,3)
//        (1,7)
    }
}

class Map_Operator {

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

class Filter_Operator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(Filter_Operator.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> javaRDD = sc.parallelize(list);

        JavaRDD<Integer> filterRDD = javaRDD.filter(v -> v > 3);
        filterRDD.foreach(v -> System.out.println(v));
    }
}

class FlatMap_Operator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(FlatMap_Operator.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<String> datas = Arrays.asList("a,1", "b,2", "c,3");
        JavaRDD<String> javaRDD = sc.parallelize(datas);
        JavaRDD<String> flatMap = javaRDD.flatMap(s -> Arrays.asList(s.split(",")).iterator());
        flatMap.foreach(s -> System.out.println(s));

    }
}


class MapPartitions_Operator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(MapPartitions_Operator.class.getSimpleName())
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

class MapPartitionsWithIndex_Operator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(MapPartitionsWithIndex_Operator.class.getSimpleName())
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

class GroupByKey_Operator {
    public static void main(String[] args) {
        List<Tuple2<Object, Object>> list = Arrays.asList(new Tuple2<Object, Object>('a', 1),
                new Tuple2<Object, Object>('b', 2),
                new Tuple2<Object, Object>('b', 2.5),
                new Tuple2<Object, Object>('c', 3));
        JavaPairRDD<Object, Object> pairRDD = sc.parallelizePairs(list);
        pairRDD.foreach(v -> System.out.println(v._1 + ": " + v._2));

        pairRDD.groupByKey().foreach(tuple -> {
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

class Distinct_Operator {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName(Distinct_Operator.class.getSimpleName()).master("local").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 4),
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 1),
                new Tuple2<>("c", 1)
        );
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(list);
        // distinct
        JavaPairRDD<String, Integer> distinct = pairRDD.distinct();
        distinct.foreach(s -> System.out.println(s));

        // distinct operator customized
        JavaPairRDD<Tuple2<String, Integer>, Object> tuple2JavaPairRDD = pairRDD.mapToPair(tuple -> new Tuple2<>(new Tuple2<>(tuple._1, tuple._2), null));
        tuple2JavaPairRDD.groupByKey().foreach(t -> System.out.println(t._1));

        sc.close();
    }
}

/**
 * 对RDD进行抽样，其中参数withReplacement为true时表示抽样之后还放回，可以被多次抽样，false表示不放回；
 * fraction表示抽样比例；seed为随机数种子，比如当前时间戳
 */
class Sample_Operator {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("sample operator").master("local").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> parallelize = jsc.parallelize(ints);
        JavaRDD<Integer> sample = parallelize.sample(false, 0.8, System.currentTimeMillis());
        sample.foreach(i -> System.out.println(i));
    }
}

/**
 * 合并两个RDD，不去重，要求两个RDD中的元素类型一致
 */
class Union_Operator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Union_Operator.class.getSimpleName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> datas1 = Arrays.asList("张三", "李四");
        List<String> datas2 = Arrays.asList("tom", "gim");

        JavaRDD<String> data1RDD = sc.parallelize(datas1);
        JavaRDD<String> data2RDD = sc.parallelize(datas2);

        JavaRDD<String> unionRDD = data1RDD
                .union(data2RDD);

        unionRDD.foreach(v -> System.out.println(v));

        sc.close();
    }
}

/**
 *返回两个RDD的交集
 */
class Intersection_Operator {
    public static void main(String[] args) {
        List<String> datas1 = Arrays.asList("张三", "李四", "tom");
        List<String> datas2 = Arrays.asList("tom", "gim");

        sc.parallelize(datas1)
                .intersection(sc.parallelize(datas2))
                .foreach(v -> System.out.println(v));
    }
}

class Join_Operations {
    public static void main(String[] args) {
        List<Tuple2<Integer, String>> products = new ArrayList<>();
        products.add(new Tuple2<>(1, "苹果"));
        products.add(new Tuple2<>(2, "梨"));
        products.add(new Tuple2<>(3, "香蕉"));
        products.add(new Tuple2<>(4, "石榴"));

        List<Tuple2<Integer, Integer>> counts = new ArrayList<>();
        counts.add(new Tuple2<>(1, 7));
        counts.add(new Tuple2<>(2, 3));
        counts.add(new Tuple2<>(3, 8));
        counts.add(new Tuple2<>(4, 3));
        counts.add(new Tuple2<>(5, 9));

        JavaPairRDD<Integer, String> productsRDD = sc.parallelizePairs(products);
        JavaPairRDD<Integer, Integer> countsRDD = sc.parallelizePairs(counts);

        productsRDD.join(countsRDD)
                .foreach(v -> System.out.println(v));
    }
}

/**
 * cogroup 扩分区
 */
class Cogroup_Operators {
    public static void main(String[] args) {
        List<Tuple2<Integer, String>> datas1 = new ArrayList<>();
        datas1.add(new Tuple2<>(1, "苹果"));
        datas1.add(new Tuple2<>(2, "梨"));
        datas1.add(new Tuple2<>(3, "香蕉"));
        datas1.add(new Tuple2<>(4, "石榴"));

        List<Tuple2<Integer, Integer>> datas2 = new ArrayList<>();
        datas2.add(new Tuple2<>(1, 7));
        datas2.add(new Tuple2<>(2, 3));
        datas2.add(new Tuple2<>(3, 8));
        datas2.add(new Tuple2<>(4, 3));


        List<Tuple2<Integer, String>> datas3 = new ArrayList<>();
        datas3.add(new Tuple2<>(1, "7"));
        datas3.add(new Tuple2<>(2, "3"));
        datas3.add(new Tuple2<>(3, "8"));
        datas3.add(new Tuple2<>(4, "3"));
        datas3.add(new Tuple2<>(4, "4"));
        datas3.add(new Tuple2<>(4, "5"));
        datas3.add(new Tuple2<>(4, "6"));

        sc.parallelizePairs(datas1)
                .cogroup(sc.parallelizePairs(datas2),
                        sc.parallelizePairs(datas3))
                .foreach(v -> System.out.println(v));
    }
}

class SortByKey {
    public static void main(String[] args) {
        List<Integer> datas = Arrays.asList(60, 70, 80, 55, 45, 75);

//    sc.parallelize(datas)
//            .sortBy(new Function<Integer, Object>() {
//                @Override
//                public Object call(Integer v1) throws Exception {
//                    return v1;
//                }
//            }, true, 1)
//            .foreach(v -> System.out.println(v));

        sc.parallelize(datas)
                .sortBy((Integer v1) -> v1, false, 1)
                .foreach(v -> System.out.println(v));

        List<Tuple2<Integer, Integer>> datas2 = new ArrayList<>();
        datas2.add(new Tuple2<>(3, 3));
        datas2.add(new Tuple2<>(2, 2));
        datas2.add(new Tuple2<>(1, 4));
        datas2.add(new Tuple2<>(2, 3));

        sc.parallelizePairs(datas2)
                .sortByKey(false)
                .foreach(v -> System.out.println(v));
    }
}

/**
 * Cartesian product
 */
class Cartesian {
    public static void main(String[] args) {
        List<String> names = Arrays.asList("张三", "李四", "王五");
        List<Integer> scores = Arrays.asList(60, 70, 80);

        JavaRDD<String> namesRDD = sc.parallelize(names);
        JavaRDD<Integer> scoreRDD = sc.parallelize(scores);

        JavaPairRDD<String, Integer> cartesianRDD = namesRDD.cartesian(scoreRDD);
        cartesianRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + "\t" + t._2());
            }
        });
    }
}

    