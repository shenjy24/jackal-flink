package com.jonas.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {

    private static final String host = "127.0.0.1";
    private static final int port = 9000;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream(host, port, "\n");
        DataStream<WordCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    out.collect(new WordCount(word, 1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1)).sum("count");
        windowCounts.print().setParallelism(1);
        env.execute("socket window count");
    }
}
