package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author: keguang
 * @Date: 2022/1/21 17:57
 * @version: v1.0.0
 * @description:
 */
public class DataSetDemo {

    public static void main(String[] args) throws Exception {
        // 创建Flink运行的上下文环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建DataSet，这里我们的输入是一行一行的文本
        DataSet<String> text = env.fromElements(
                "Flink Spark Storm",
                        "Flink Flink Flink",
                        "Spark Spark Spark",
                        "Storm Storm Storm"
        );
        // 通过Flink内置的转换函数进行计算
        DataSet<Tuple2<String, Integer>> counts =
                text.flatMap(new LineSplitter())
                        .groupBy(0)
                        .sum(1).setParallelism(1);
        //结果打印
        counts.print();

        // env.execute("DataSetDemo");
    }

    private static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for(String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
