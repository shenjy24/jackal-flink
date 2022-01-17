package com.jonas.api.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 接收Socket传输过来的数据，将数据保存到Redis中
 *
 * @author shenjy
 * @version 1.0
 * @date 2022-01-17
 */
public class StreamingToRedis {

    private static final String host = "127.0.0.1";
    private static final int port = 9000;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream(host, port, "\n");
        DataStream<Tuple2<String, String>> words = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("words", value);
            }
        });

        //创建Redis的配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost(host).setPort(6379).build();
        //创建RedisSink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(config, new RedisMapper<Tuple2<String, String>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.LPUSH);
            }

            @Override
            public String getKeyFromData(Tuple2<String, String> data) {
                return data.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, String> data) {
                return data.f1;
            }
        });

        words.addSink(redisSink);
        env.execute("streaming to redis");
    }
}
