package com.aura.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class StreamMytest2 {

    public static void main(String[] args){
        if (args== null || args.length < 0){
            System.err.println("main 函数参数错误！");
        }

        SparkConf conf = new SparkConf().setAppName("StreamMytest2");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(60));

        JavaReceiverInputDStream<String> receiverInputDStream = ssc.socketTextStream(args[0], Integer.parseInt(args[1]), StorageLevel.MEMORY_AND_DISK());

        JavaDStream<String> window = receiverInputDStream.window(Durations.minutes(5)
                , Durations.seconds(60));

        JavaDStream<String> dStream = window.flatMap(x ->
                Arrays.asList(x.split(" ")).iterator()
        );

        JavaPairDStream<String, Integer> count = dStream.
                mapToPair(x -> new Tuple2<>(x, 1)).
                reduceByKey((a, b) -> a + b);


        count.print();
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
