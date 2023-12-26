package window;

import common.Person;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口函数
 */
public class WindowFunctionDemo {

    public static void main(String[] args) throws Exception {
        reduceFunctionDemo();
    }

    public static void reduceFunctionDemo() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Person> persons = senv.fromElements(
                new Person("张三", 30)
                , new Person("张三", 30)
                , new Person("李四", 40)
                , new Person("李四", 40)
                , new Person("王五", 50)
                , new Person("王五", 50));

        persons.keyBy(item -> item.getName())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)))
                .reduce(new ReduceFunction<Person>() {
                    @Override
                    public Person reduce(Person value1, Person value2) throws Exception {
                        System.out.println(value1);
                        return new Person(value1.getName(), value1.getAge() + value2.getAge());
                    }
                }).print();

        senv.execute();
        
    }
}
