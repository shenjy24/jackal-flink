package com.jonas.example;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BatchCache {

    private static final String FILE_NAME = "output";
    private static final String FILE_PATH = "D:\\java\\workspace\\jackal-flink\\src\\main\\resources\\output";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //注册文件
        env.registerCachedFile(FILE_PATH, FILE_NAME);
        DataSource<String> data = env.fromElements("a", "b", "c", "d");
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            private List<String> dataList = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //获取文件
                File file = getRuntimeContext().getDistributedCache().getFile(FILE_NAME);
                List<String> lines = FileUtils.readLines(file);
                for (String line : lines) {
                    dataList.add(line);
                    System.out.println("line:" + line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                //在这里就可以使用dataList
                return value;
            }
        });
        result.print();
    }
}
