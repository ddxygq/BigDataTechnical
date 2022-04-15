import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: keguang
 * @Date: 2022/1/24 17:55
 * @version: v1.0.0
 * @description: 侧输出
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();

        data.add(new Tuple3<>(0,1,0));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,3));
        data.add(new Tuple3<>(1,2,5));
        data.add(new Tuple3<>(1,2,9));
        data.add(new Tuple3<>(1,2,11));
        data.add(new Tuple3<>(1,2,13));

        DataStreamSource<Tuple3<Integer,Integer,Integer>> items = senv.fromCollection(data);

        OutputTag<Tuple3<Integer, Integer, Integer>> zeroOutput = new OutputTag<Tuple3<Integer, Integer, Integer>>("zeroOutput"){};
        OutputTag<Tuple3<Integer, Integer, Integer>> oneOutput = new OutputTag<Tuple3<Integer, Integer, Integer>>("oneOutput"){};
        OutputTag<Tuple3<Integer, Integer, Integer>> otherOutput = new OutputTag<Tuple3<Integer, Integer, Integer>>("otherOutput"){};

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> processStream= items.process(new ProcessFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {

            @Override
            public void processElement(Tuple3<Integer, Integer, Integer> value, Context ctx, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {

                if (value.f0 == 0) {
                    ctx.output(zeroOutput, value);

                } else if (value.f0 == 1) {
                    ctx.output(oneOutput, value);

                } else {
                    ctx.output(otherOutput, value);
                }
            }

        });

        DataStream zeroStream = processStream.getSideOutput(zeroOutput);
        DataStream oneStream = processStream.getSideOutput(zeroOutput);
        DataStream otherStream = processStream.getSideOutput(zeroOutput);

        zeroStream.print();
        oneStream.print();
        otherStream.print();

        senv.execute("SideOutputDemo");
    }

}
