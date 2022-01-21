package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: keguang
 * @Date: 2022/1/21 18:14
 * @version: v1.0.0
 * @description:
 */
public class DataStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 监听9000端口
        DataStream<String> text = senv.socketTextStream("cdh-001", 9999, "\n");

        DataStream<WordWithCount> windowsCounts = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                        for(String word : s.split("\\s")) {
                            collector.collect(new WordWithCount(word, 1));
                        }
                    }
                }).keyBy(new KeySelector<WordWithCount, String>() {
                    @Override
                    public String getKey(WordWithCount wordWithCount) throws Exception {
                        return wordWithCount.word;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount wordWithCount, WordWithCount wordWithCount2) throws Exception {
                        return new WordWithCount(wordWithCount.word, wordWithCount.count + wordWithCount2.count);
                    }
                });

        windowsCounts.print();

        senv.execute("DataStreamDemo");


    }


    private static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {

            this.word = word;
            this.count = count;

        }

        @Override
        public String toString() {
            return word + " : " + count;
        }

    }
}
