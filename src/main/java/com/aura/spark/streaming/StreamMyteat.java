package com.aura.spark.streaming;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class StreamMyteat {
    private static SparkSession instance = null;

    public static SparkSession getInstance(SparkConf conf) {
        if (instance == null) {
            instance = SparkSession.builder().config(conf).getOrCreate();
        }
        return instance;
    }


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
        JavaDStream<String> window = lines.window(Durations.minutes(5), Durations.seconds(60));
        JavaDStream<String> words = window.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] s1 = s.split(" ");
                Iterator<String> iterator = Arrays.asList(s1).iterator();
                return iterator;
            }
        });

       words.foreachRDD((rdd,time)->{
           SparkConf conf1 = rdd.context().getConf();
           SparkSession session = getInstance(conf1);
           JavaRDD<Record> recordJavaRDD = rdd.map(x -> new Record(x));
           Dataset<Row> dataFrame = session.createDataFrame(recordJavaRDD, Record.class);
           dataFrame.createOrReplaceTempView("word");
           Dataset<Row> wordData = session.sql("select word,count(*) as count from word group by word");
           wordData.show();
       });

        //JavaPairDStream<String, Integer> objectObjectJavaPairDStream = words.mapToPair(x -> new Tuple2(x, 1));
        //JavaPairDStream<String, Integer> wordCounts = objectObjectJavaPairDStream.reduceByKey((a, b) -> a + b);

        //wordCounts.print();

        //wordCounts.saveAsHadoopFiles("hdfs://master:9000/tmp/output","spark",String.class,String.class, TextOutputFormat.class);

        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
