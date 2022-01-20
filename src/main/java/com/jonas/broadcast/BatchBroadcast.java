package com.jonas.broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BatchBroadcast {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs", 18));
        broadData.add(new Tuple2<>("ls", 20));
        broadData.add(new Tuple2<>("ww", 17));
        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);

        DataSet<HashMap<String, Integer>> toBroadcast = tupleData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String, Integer> res = new HashMap<>();
                res.put(value.f0, value.f1);
                return res;
            }
        });

        DataSource<String> data = env.fromElements("zs", "ls", "ww");
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            List<HashMap<String, Integer>> broadcastMap = new ArrayList<>();
            HashMap<String, Integer> allMap = new HashMap<>();

            @Override
            public String map(String val) throws Exception {
                Integer age = allMap.get(val);
                return val + "," + age;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                broadcastMap = getRuntimeContext().getBroadcastVariable("broadcastMapName");
                for (HashMap map : broadcastMap) {
                    allMap.putAll(map);
                }
            }
        }).withBroadcastSet(toBroadcast, "broadcastMapName");

        result.print();
    }
}
