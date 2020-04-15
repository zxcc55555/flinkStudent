package com.flink.student;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SourceFunction<Transaction> sourceFunction = new TransactionSource();
        //创建数据源
        DataStream<Transaction> transactions = env
            .addSource(new TransactionSource())
            //数据源名称
            .name("transactions");

        DataStream<Alert> alerts = transactions
            //对事件分区
            .keyBy(Transaction::getAccountId)
            //对分区后的数据进行处理
            .process(new FraudDetector())
            //操作名称
            .name("fraud-detector");
        //对处理后的数据写到外部系统例如kafka
        alerts
            .addSink(new AlertSink())
            .name("send-alerts");
        //运行任务，取任务名
        env.execute("Fraud Detection");
    }
}