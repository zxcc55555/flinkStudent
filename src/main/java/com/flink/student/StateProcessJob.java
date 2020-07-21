package com.flink.student;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class StateProcessJob {
    private static List<Integer> data = Lists.newArrayList(1,2,3,4,5);
    public static void main(String[] args) throws Exception {
        state();
        processTimeJob();
    }
    private static void state() throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> dataStreamSource = e.fromCollection(data);
        KeyedStream<Integer, Integer> integerIntegerKeyedStream = dataStreamSource.keyBy(v -> v % 2);
        integerIntegerKeyedStream.process(new KeyedProcessFunction<Integer, Integer, Integer>() {
            private ValueState<Integer> sumState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>(
                        "sum",
                        Integer.class);
                sumState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                Integer oldSum = sumState.value();
                int sum = oldSum == null ? 0 : oldSum;
                sum += value;
                sumState.update(sum);
                out.collect(sum);
            }
        }).print().setParallelism(2);
        e.execute();
    }

    public static void processTimeJob() throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = e.addSource(new SourceFunction<Integer>() {
            private volatile boolean stop = false;
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                int i = 0;
                while (!stop && i < data.size()) {
                    ctx.collect(data.get(i++));
                    Thread.sleep(200);
                }

            }

            @Override
            public void cancel() {
                stop = true;
            }
        }).setParallelism(1);
        e.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        source.keyBy(v -> v % 2).process(new KeyedProcessFunction<Integer, Integer, Integer>() {
            private static final int WINDOW_SIZE = 400;
            private TreeMap<Long, Integer> windows;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                windows = new TreeMap<>();
            }

            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                long currentTime = ctx.timerService().currentProcessingTime();
                long windowStart = currentTime / WINDOW_SIZE;
                //update the window
                int sum = windows.getOrDefault(windowStart, 0);
                windows.put(windowStart, sum + value);

                //fire old windows
                Map<Long, Integer> oldWindow = windows.headMap(windowStart, false);
                Iterator<Map.Entry<Long, Integer>> iterator = oldWindow.entrySet().iterator();
                while(iterator.hasNext()){
                    out.collect(iterator.next().getValue());
                    iterator.remove();
                }
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("windows:" + windows.toString());
            }
        }).print().setParallelism(2);
        e.execute();
    }
}
