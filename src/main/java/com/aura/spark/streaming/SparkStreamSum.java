package com.aura.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Option;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamSum {

    public static JavaStreamingContext createContext(String checkPointDir,String host, int port ){
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamSum");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(30));
        /* 設置檢查點 */
        ssc.checkpoint(checkPointDir);
        ssc.sparkContext().setLogLevel("WARN");

        JavaReceiverInputDStream<String> inputDStream = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK());
        JavaDStream<String> flatMap = inputDStream.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> mapToPair = flatMap.mapToPair(x -> new Tuple2<String, Integer>(x, 1));

        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> countDstream = mapToPair.mapWithState(StateSpec.function(new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<Integer> state) throws Exception {
                Option<Integer> stateCount = state.getOption();
                Integer sum = one.orElse(0);
                if (stateCount.isDefined()) {
                    sum += stateCount.get();
                }
                state.update(sum);
                return new Tuple2<String, Integer>(word, sum);
            }
        }));

        mapToPair.print();
        countDstream.print();
        countDstream.stateSnapshots().print();

        return ssc;
    }

    public static void  main(String[] args) throws InterruptedException {
        if (args == null || args.length <3){
            System.out.println("参数错误！");
            System.exit(1);
        }
        String checkPointDir = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);

        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkPointDir, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                JavaStreamingContext streamingContext = createContext(checkPointDir, host, port);
                return streamingContext;
            }
        });
        ssc.start();
        ssc.awaitTermination();
    }
}
