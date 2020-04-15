package com.flink.student;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.walkthrough.common.table.BoundedTransactionTableSource;
import org.apache.flink.walkthrough.common.table.SpendReportTableSink;
import org.apache.flink.walkthrough.common.table.TruncateDateToHour;

public class SpendReport {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        tEnv.registerTableSource("transactions", new BoundedTransactionTableSource());
        tEnv.registerTableSink("spend_report", new SpendReportTableSink());
        tEnv.registerFunction("truncateDateToHour", new TruncateDateToHour());

        tEnv
                .scan("transactions")
                .insertInto("spend_report");

        env.execute("Spend Report");
    }
}
