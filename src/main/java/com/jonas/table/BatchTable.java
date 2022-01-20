package com.jonas.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class BatchTable {

    private static final String path = "D:\\java\\workspace\\jackal-flink\\src\\main\\resources\\";

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.getTableEnvironment(env);

        tEnv.connect(new FileSystem().path(path + "table_input"))
                .withFormat(new Csv().field("word", Types.STRING).lineDelimiter("\n"))
                .withSchema(new Schema().field("word", Types.STRING))
                .registerTableSource("fileSource");

        Table result = tEnv.scan("fileSource")
                .groupBy("word")
                .select("word, count(1) as count");

        TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING, Types.LONG);
        DataSet<Tuple2<String, Integer>> dataSet = tEnv.toDataSet(result, tupleType);
        dataSet.writeAsCsv(path + "table_output", "\n", " ").setParallelism(1);
        env.execute("batch table");
    }
}
