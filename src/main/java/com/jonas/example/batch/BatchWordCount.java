package com.jonas.example.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class BatchWordCount {
    private static final String path = " D:\\java\\workspace\\jackal-flink\\src\\main\\resources\\";

    public static void main(String[] args) throws Exception {
        countWord("input", "output");
    }

    public static void countWord(String input, String output) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.readTextFile(path + input);
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        counts.writeAsCsv(path + output, "\n", " ").setParallelism(1);
        env.execute("batch word count");
    }
}
