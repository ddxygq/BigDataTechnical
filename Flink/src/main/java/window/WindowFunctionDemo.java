package window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口函数
 */
public class WindowFunctionDemo {

    public static void main(String[] args) throws Exception {
        // reduceFunctionDemo();

        // aggregateFunctionDemo();

        processWindowFunctionDemo();
    }

    public static void reduceFunctionDemo() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = senv.socketTextStream("192.168.20.130", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] values = value.split(" ");
                        for(String v : values) {
                            out.collect(Tuple2.of(v, 1));
                        }
                    }
                });
        dataStream.keyBy(item -> item.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(3000)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print();

        senv.execute("reduceFunctionDemo");
        
    }

    public static void aggregateFunctionDemo() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = senv.socketTextStream("192.168.20.130", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] values = value.split(" ");
                        for(String v : values) {
                            out.collect(Tuple2.of(v, 1));
                        }
                    }
                });
        dataStream.keyBy(item -> item.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(3000)))
                .aggregate(new AggregateFunction<Tuple2<String,Integer>, Tuple2<Integer, Integer>, Float>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return new Tuple2<>(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(Tuple2<String, Integer> value, Tuple2<Integer, Integer> accumulator) {
                        return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1);
                    }

                    @Override
                    public Float getResult(Tuple2<Integer, Integer> accumulator) {
                        return (float) accumulator.f0 / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                    }
                })
                .print();

        senv.execute("aggregateFunctionDemo");

    }

    public static void processWindowFunctionDemo() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = senv.socketTextStream("192.168.20.130", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] values = value.split(" ");
                        for(String v : values) {
                            out.collect(Tuple2.of(v, 1));
                        }
                    }
                });
        dataStream.keyBy(item -> item.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(3000)))
                .process(new ProcessWindowFunction<Tuple2<String,Integer>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        long count = 0;
                        for (Tuple2<String, Integer> in: elements) {
                            count ++;
                        }
                        out.collect("key: " + key + ", Window: " + context.window() + ", count: " + count);
                    }
                })
                .print();

        senv.execute("processWindowFunctionDemo");

    }
}
