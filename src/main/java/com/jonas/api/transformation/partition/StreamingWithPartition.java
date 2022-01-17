package com.jonas.api.transformation.partition;

import com.jonas.api.source.NoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingWithPartition {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Long> text = env.addSource(new NoParallelSource());

        DataStream<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });
        //分区之后的数据
        DataStream<Tuple1<Long>> partitionData = tupleData.partitionCustom(new Partition(), 0);
        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程id:" + Thread.currentThread().getId() + ", value:" + value);
                return value.getField(0);
            }
        });
        result.print().setParallelism(1);

        env.execute("streaming with partition");
    }
}
