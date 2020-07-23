package com.flink.student;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.*;

public class ProcessTimeJob {
    private static List<Integer> data = Lists.newArrayList(1, 2, 3, 4, 5);

    public static void main(String[] args) throws Exception {
        state();
    }

    private static void state() throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, Long>> dataStreamSource = e.addSource(new SourceFunction<Tuple2<Integer, Long>>() {
            private volatile boolean stop = false;
            private Random random = new Random();

            @Override
            public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
                int i = 0;
                while (!stop && i < data.size()) {
                    ctx.collectWithTimestamp(
                            Tuple2.of(data.get(i++), System.currentTimeMillis()),
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
        KeyedStream<Tuple2<Integer, Long>, Integer> integerIntegerKeyedStream = dataStreamSource.keyBy(v -> v.f0 % 2);
        integerIntegerKeyedStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(4)))
                .process(new ProcessAllWindowFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>, TimeWindow>() {
                    MapState<Long, Integer> mapState;


                    @Override
                    public void process(Context context, Iterable<Tuple2<Integer, Long>> elements, Collector<Tuple2<Integer, Long>> out) throws Exception {
                        Iterator<Tuple2<Integer, Long>> iterator = elements.iterator();
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        MapStateDescriptor<Long, Integer> mapStateDescriptor = new MapStateDescriptor<>(
                                "sum",
                                Types.LONG,
                                Types.INT
                        );
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public void clear(Context context) throws Exception {
                        super.clear(context);
                        mapState.clear();
                    }
                });
    }

    private static void event() throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, Long>> dataStreamSource = e.addSource(new SourceFunction<Tuple2<Integer, Long>>() {
            private volatile boolean stop = false;
            private Random random = new Random();

            @Override
            public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
                int i = 0;
                while (!stop && i < data.size()) {
                    ctx.collectWithTimestamp(
                            Tuple2.of(data.get(i++), System.currentTimeMillis()),
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
        dataStreamSource.
                assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<Integer, Long>>() {
            private final long maxTimeLag = 4000;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis() - maxTimeLag);
            }

            @Override
            public long extractTimestamp(Tuple2<Integer, Long> element, long previousElementTimestamp) {
                return element.f1;
            }
        }).keyBy(v -> v.f0 % 2).process(new KeyedProcessFunction<Integer, Tuple2<Integer, Long>, Tuple2<Integer, Long>>() {
            MapState<Long, Integer> mapState;
            @Override
            public void processElement(Tuple2<Integer, Long> value, Context ctx, Collector<Tuple2<Integer, Long>> out) throws Exception {
                long watermark = ctx.timerService().currentWatermark();
                Integer integer = mapState.get(watermark);
                integer = integer == null ? 0 : integer;
                mapState.put(watermark, integer + value.f0);
                out
            }
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                MapStateDescriptor<Long, Integer> mapStateDescriptor = new MapStateDescriptor<>(
                        "sum",
                        Types.LONG,
                        Types.INT
                );
                mapState = getRuntimeContext().getMapState(mapStateDescriptor);
            }
        }).print().setParallelism(2);
        e.execute("evementTime");
    }
}
