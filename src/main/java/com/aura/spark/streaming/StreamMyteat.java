package com.aura.spark.streaming;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Tuple2$mcCC$sp;

import java.util.Arrays;
import java.util.Iterator;

public class StreamMyteat {
    public static void main(String [] args){
        if ( args.length <0 ){
            System.err.println(" Usage: StreamingWordCount <hostname> <port> ");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("StreamMyteat");
        //JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0],
                Integer.parseInt(args[1]), StorageLevel.MEMORY_AND_DISK());

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] s1 = s.split(" ");
                Iterator<String> iterator = Arrays.asList(s1).iterator();
                return iterator;
            }
        });

        JavaPairDStream<String, Integer> objectObjectJavaPairDStream = words.mapToPair(x -> new Tuple2(x, 1));
        JavaPairDStream<String, Integer> wordCounts = objectObjectJavaPairDStream.reduceByKey((a, b) -> a + b);

        wordCounts.print();

        wordCounts.saveAsHadoopFiles("hdfs://master:9000/tmp/output","spark",String.class,String.class, TextOutputFormat.class);

        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
