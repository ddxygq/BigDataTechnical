package watermark;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: keguang
 * @Date: 2022/1/24 17:00
 * @version: v1.0.0
 * @description:
 */
public class WaterMarkDemo {

    public static void main(String[] args) throws Exception {
        demo();
    }

    public static void demo() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每条数据最多延迟 5s
        long maxOutOfOrderness = 5000;
        // 窗口大小5s
        long windowSize = 5000;

        DataStream<Tuple2<String, Integer>> dataStream = senv.socketTextStream("192.168.20.130", 9999)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(maxOutOfOrderness))
                        .withTimestampAssigner((event, timestamp) ->
                                // 获取数据里面的time字段
                            JSON.parseObject(event).getLong("time")
                        )).map(item -> Tuple2.of(JSON.parseObject(item).getString("name"), 1));
        dataStream.keyBy(item -> item.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .sum(1)
                .print();

        senv.execute("reduceFunctionDemo");

    }
}
