package com.jonas.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class BatchAccumulator {

    private static final String ACCUMULATOR_NAME = "num-lines";
    private static final String output = "D:\\java\\workspace\\jackal-flink\\src\\main\\resources\\accumulator_count";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("a", "b", "c", "d");
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            //创建累加器
            private IntCounter counter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //注册累加器
                getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, counter);
            }

            //int sum = 0;
            @Override
            public String map(String value) throws Exception {
                //如果并行度为1，则使用普通的累加求和即可；
                //如果设置多个并行度，则普通的求和结果就不准确
                //sum++;
                //System.out.println("sum:" + sum);

                counter.add(1);
                return value;
            }
        }).setParallelism(8);

        result.writeAsText(output);

        JobExecutionResult jobExecutionResult = env.execute("counter");
        //获取累加器
        int num = jobExecutionResult.getAccumulatorResult(ACCUMULATOR_NAME);
        System.out.println("num:" + num);
    }
}
