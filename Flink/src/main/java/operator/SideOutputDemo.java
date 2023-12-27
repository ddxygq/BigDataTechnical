package operator;

import common.Person;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 旁路輸出
 */
public class SideOutputDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> persons = senv.socketTextStream("192.168.20.130", 9999)
                .map(line -> new Person(line.split(",")[0], Integer.valueOf(line.split(",")[1]))).setParallelism(1);

        // 这需要是一个匿名的内部类，以便我们分析类型
        OutputTag<String> outputTag = new OutputTag<String>("side-output") {};

        SingleOutputStreamOperator<Person> result = persons.process(new ProcessFunction<Person, Person>() {
            @Override
            public void processElement(Person value, Context ctx, Collector<Person> out) throws Exception {
                // 发送数据到主要的输出
                out.collect(value);

                // 发送数据到旁路输出
                if(value.getAge() == 28) {
                    ctx.output(outputTag, value.toString() + "==============");
                }
            }
        });

        DataStream<String> outPutStream = result.getSideOutput(outputTag);
        outPutStream.print();
        result.print();

        senv.execute("SideOutputDemo");
    }
}
