package com.jonas.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

public class BatchSql {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        DataSource<String> dataSource = env.readTextFile("D:\\java\\workspace\\jackal-flink\\src\\main\\resources\\student");
        DataSet<Student> inputData = dataSource.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String s) throws Exception {
                String[] splits = s.split(",");
                return new Student(splits[0], Integer.parseInt(splits[1]));
            }
        });
        //将DataSet转换为Table
        Table table = tableEnv.fromDataSet(inputData);
        //注册student表
        tableEnv.registerTable("student", table);

        //执行sql查询
        Table sqlQuery = tableEnv.sqlQuery("select count(1), avg(age) from student");
        //创建CsvTableSink
        String output = "D:\\java\\workspace\\jackal-flink\\src\\main\\resources\\student_result";
        CsvTableSink csvTableSink = new CsvTableSink(output, ",", 1, FileSystem.WriteMode.OVERWRITE);
        //注册TableSink
        tableEnv.registerTableSink("csvOutputTable", new String[]{"count", "avg_age"}, new TypeInformation[]{Types.LONG, Types.INT}, csvTableSink);
        //把数据结果添加到Sink中
        sqlQuery.insertInto("csvOutputTable");
        env.execute("sql batch");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private String name;
        private int age;
    }
}
