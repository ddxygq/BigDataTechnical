package watermark;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置为eventtime事件类型
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        senv.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        DataStreamSource<String> dataStream = senv.addSource(new SourceFromFile());

        long maxOutOfOrderness = 5 * 1000L;
        DataStream<Tuple2<String, Long>> dataStream1 = dataStream.filter(item -> {
            return item.split(",").length == 11;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(maxOutOfOrderness))
                .withTimestampAssigner((element, recordTimestamp) -> {
                    System.out.println("data is -> " + element);
                    // 时间戳须为毫秒
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    Date date = null;
                    try {
                        date = sdf.parse(element.split(",")[0]);
                    } catch (ParseException e) {
                        date = new Date();
                        e.printStackTrace();
                    }
                    return date.getTime();
                })).map(item -> {
                    String[] arr = item.split(",");
            System.out.println("data -> " + arr[7] + arr[8] + arr[9] + arr[10]);
                    return Tuple2.of((String) JSON.parseObject(arr[7] + arr[8] + arr[9] + arr[10]).get("level"), 1L);
        }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));
        dataStream1.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5))).sum(1).print();

        senv.execute();
    }
}
