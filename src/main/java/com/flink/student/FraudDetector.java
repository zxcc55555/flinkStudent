package com.flink.student;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * @author 60238
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;
    /**
     * 用来判断是否是欺诈行为
     */
    private transient ValueState<Boolean> flagState;
    /**
     * 时间状态
     */
    private transient ValueState<Long> timerState;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer-state",
                Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        // 获取当前flagState的状态
        Boolean lastTransactionWasSmall = flagState.value();

        // 验证状态是否存在状态，存在则说明上一次是小金额
        if (lastTransactionWasSmall != null) {
            //验证此次金额是否为大金额
            if (transaction.getAmount() > LARGE_AMOUNT) {
                // 输出警报
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }

            // 清除状态
            cleanUp(context);
        }
        //如果金额过小
        if (transaction.getAmount() < SMALL_AMOUNT) {
            // 修改状态为true
            flagState.update(true);
            // 设置计时器和计时器状态
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }
    }
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        // 1分钟后删除标记
        timerState.clear();
        flagState.clear();
    }
    private void cleanUp(Context ctx) throws Exception {
        // 删除 timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // 删除标记
        timerState.clear();
        flagState.clear();
    }
}
