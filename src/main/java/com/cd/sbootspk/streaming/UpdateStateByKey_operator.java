/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cd.sbootspk.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.googlecode.aviator.AviatorEvaluator;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;

/**
 * Counts words cumulatively in UTF8 encoded, '\n' delimited text received from the network every
 * second starting with initial value of word count.
 * Usage: JavaStatefulNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
 * data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaStatefulNetworkWordCount localhost 9999`
 */
public class UpdateStateByKey_operator {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {


        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("JavaStatefulNetworkWordCount").setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));
        ssc.checkpoint("D:\\Idea_Workspace\\checkpoint");

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("cluster01", 9999, StorageLevels.MEMORY_AND_DISK_SER_2);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(s -> new Tuple2<>(s, 1));

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                (values, state) -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("list", values);
                    Long sum = (Long) AviatorEvaluator.execute("reduce(list,+,0)", map);
                    Integer newSum = sum.intValue() + state.orElse(0); // add the new values with the previous running count to get the new count
                    return Optional.of(newSum);
                };
        JavaPairDStream<String, Integer> runningCounts = wordsDstream.updateStateByKey(updateFunction);

        //  create a single connection object and send all the records in a RDD partition using that connection.
//        runningCounts.foreachRDD(rdd -> {
//            rdd.foreachPartition(partitionOfRecords -> {
//                Connection connection = createNewConnection();
//                while (partitionOfRecords.hasNext()) {
//                    connection.send(partitionOfRecords.next());
//                }
//                connection.close();\
//            });
//        });


        runningCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }
}