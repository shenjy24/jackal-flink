package com.jonas.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

public class StreamTable {

    private static final String input = "D:\\java\\workspace\\jackal-flink\\src\\main\\resources\\student";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        TableSource csvSource = new CsvTableSource(input, new String[]{"name", "age"}, new TypeInformation[]{Types.STRING, Types.INT});
        tableEnv.registerTableSource("CsvTable", csvSource);
        Table csvTable = tableEnv.scan("CsvTable");
        Table csvResult = csvTable.select("name,age");
        DataStream<Student> csvStream = tableEnv.toAppendStream(csvResult, Student.class);
        csvStream.print().setParallelism(1);

        env.execute("stream table");
    }
}
