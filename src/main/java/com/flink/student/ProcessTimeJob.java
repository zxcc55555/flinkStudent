package com.flink.student;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class ProcessTimeJob {
    private static List<Integer> data = Lists.newArrayList(1,2,3,4,5);
    public static void main(String[] args) throws Exception {
        state();
    }
    private static void state() throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer,Long>> dataStreamSource = e.addSource(new SourceFunction<Tuple2<Integer,Long>>() {
            private volatile boolean stop = false;
            private Random random = new Random();

            @Override
            public void run(SourceContext<Tuple2<Integer,Long>> ctx) throws Exception {
                int i = 0;
                while (!stop && i < data.size()) {
                    ctx.collectWithTimestamp(
                            Tuple2.of(data.get(i++),System.currentTimeMillis()),
                            System.currentTimeMillis() - random.nextInt(500));
                    Thread.sleep(200);
                }
            }

            @Override
            public void cancel() {
                stop = true;
            }
        }).setParallelism(1);
        e.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        KeyedStream<Tuple2<Integer,Long>, Integer> integerIntegerKeyedStream = dataStreamSource.keyBy(v -> v.f0 % 2);
        integerIntegerKeyedStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<Tuple2<Integer,Long>, Tuple2<Integer,Long>, TimeWindow>() {
                    MapState<Long, Integer> mapState;
                    @Override
                    public void process(Context context, Iterable<Tuple2<Integer, Long>> elements, Collector<Tuple2<Integer, Long>> out) throws Exception {

                    }

            @Override
            public void processElement(Tuple2<Integer,Long> value, Context ctx, Collector<Tuple2<Integer,Long>> out) throws Exception {
                TimerService timerService = ctx.timerService();
                long l = timerService.currentWatermark();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                MapStateDescriptor<Long, Integer> mapStateDescriptor = new MapStateDescriptor<Long, Integer>(
                        "sum",
                        Types.LONG,
                        Types.INT
                );
                getRuntimeContext().getMapState(mapStateDescriptor);
            }
        });
    }
}
