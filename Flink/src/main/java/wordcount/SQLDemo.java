package wordcount;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: keguang
 * @Date: 2022/1/21 18:29
 * @version: v1.0.0
 * @description:
 */
public class SQLDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(senv);

        String words = "hello flink hello lagou";

        String[] split = words.split("\\W+");

        ArrayList<WC> list = new ArrayList<>();



        for(String word : split){

            WC wc = new WC(word,1);

            list.add(wc);

        }

        DataSet<WC> input = fbEnv.fromCollection(list);

    }
}
