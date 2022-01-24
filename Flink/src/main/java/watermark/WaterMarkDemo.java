package watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author: keguang
 * @Date: 2022/1/24 17:00
 * @version: v1.0.0
 * @description:
 */
public class WaterMarkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //设置为eventtime事件类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置水印生成时间间隔100ms
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> dataStream = env
                .socketTextStream("cdh-001", 9999)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    private Long currentTimeStamp = 0L;

                    //设置允许乱序时间
                    private Long maxOutOfOrderness = 5000L;

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimeStamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(String s, long l) {
                        String[] arr = s.split(",");

                        long timeStamp = Long.parseLong(arr[1]);

                        currentTimeStamp = Math.max(timeStamp, currentTimeStamp);

                        System.err.println(s + ",EventTime:" + timeStamp + ",watermark:" + (currentTimeStamp - maxOutOfOrderness));
                        return timeStamp;
                    }

                });

        dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[0], Long.parseLong(split[1]));

            }

        })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 计算每个Key的最小值
                .minBy(1)
                .print();

        env.execute("WaterMarkDemo");
    }
}
