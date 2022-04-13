package watermark;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Date;

public class Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置为eventtime事件类型
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStream = senv.addSource(new SourceFromFile());

        long maxOutOfOrderness = 5 * 1000L;
        DataStream<Tuple2<String, Long>> dataStream1 = dataStream.filter(item -> {
            return item.split(",").length == 11;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(maxOutOfOrderness))
                .withTimestampAssigner((element, recordTimestamp) -> {
                    // 时间戳须为毫秒
                    return new Date(element.split(",")[0]).getTime();
                })).map(item -> {
                    String[] arr = item.split(",");
                    return Tuple2.of((String) JSON.parseObject(arr[8] + arr[8] + arr[8] + arr[8]).get("level"), 1L);
        });
        dataStream1.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5))).sum(1).print();

        senv.execute();
    }
}
