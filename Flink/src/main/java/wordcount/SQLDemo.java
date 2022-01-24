package wordcount;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: keguang
 * @Date: 2022/1/21 18:29
 * @version: v1.0.0
 * @description:
 */
public class SQLDemo {
    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        BatchTableEnvironment tbEnv = BatchTableEnvironment.create(env);

        String words = "hello flink hello spark flink";

        String[] split = words.split("\\W+");

        List<WC> list = new ArrayList<>();
        for(String word : split){
            WC wc = new WC(word,1);
            list.add(wc);
        }

        DataSet ds =  env.fromCollection(list);
        Table table = tbEnv.fromDataSet(ds);
        table.printSchema();
        // 注册表
        tbEnv.createTemporaryView("wordcount", table);
        Table tableRes = tbEnv.sqlQuery("select word, sum(frequency) as frequency from wordcount group by word");
        tbEnv.toDataSet(tableRes, WC.class).printToErr();

    }



    public static class WC {

        public String word;
        public long frequency;

        public WC() {}

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;

        }

        @Override
        public String toString() {
            return  word + ", " + frequency;
        }
    }
}
