package com.flink.student;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.walkthrough.common.table.BoundedTransactionTableSource;
import org.apache.flink.walkthrough.common.table.SpendReportTableSink;
import org.apache.flink.walkthrough.common.table.TruncateDateToHour;

/**
 * @author 60238
 */
public class SpendReport {
    public static void main(String[] args) throws Exception{
        /*
         * 前两行设置了你的 ExecutionEnvironment。 运行环境用来设置作业的属性、指定应用是批处理还是流处理，以及创建数据源。
         * 由于你正在建立一个定时的批处理报告，本教程以批处理环境作为开始。 然后将其包装进 BatchTableEnvironment 中从而能够使用所有的 Tabel API。
         */
        ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        // source 提供对存储在外部系统中的数据的访问；例如数据库、键-值存储、消息队列或文件系统
        tEnv.registerTableSource("transactions", new BoundedTransactionTableSource());
        // sink 则将表中的数据发送到外部存储系统。 根据 source 或 sink 的类型，它们支持不同的格式，如 CSV、JSON、Avro 或 Parquet。
        tEnv.registerTableSink("spend_report", new SpendReportTableSink());
        //一个用来处理时间戳的自定义函数随表一起被注册到tEnv中。 此函数将时间戳向下舍入到最接近的小时。
        tEnv.registerFunction("truncateDateToHour", new TruncateDateToHour());

        tEnv
                .from("transactions")
                .select("accountId, timestamp.truncateDateToHour as timestamp, amount")
                .groupBy("accountId, timestamp")
                .select("accountId, timestamp, amount.sum as total")
                .insertInto("spend_report");

        env.execute("Spend Report");
    }
}
