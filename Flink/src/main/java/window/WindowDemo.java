package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class WindowDemo {
    public static void main(String[] args) throws Exception {
        // demo();

        // demo2();

        // demo3();

        demo4();
    }

    public static void demo() throws Exception {
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
                .reduce((a,b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .print();

        senv.execute("WindowDemo");
    }

    public static void demo2() throws Exception {
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
        dataStream.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(3000)))
                .reduce((a,b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .print();

        senv.execute("WindowDemo");
    }

    public static void demo3() throws Exception {
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
                .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(10 * 1000)))
                .reduce((a,b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .print();

        senv.execute("WindowDemo");
    }

    public static void demo4() throws Exception {
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
                .window(GlobalWindows.create())
                .trigger(new Trigger<Tuple2<String, Integer>, GlobalWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .reduce((a,b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .print();

        senv.execute("WindowDemo");
    }
}
