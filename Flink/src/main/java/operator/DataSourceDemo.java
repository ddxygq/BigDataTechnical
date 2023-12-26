package operator;

import common.Person;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;

public class DataSourceDemo {

    public static void main(String[] args) throws Exception {
        // demo();
        // demo2();
        // demo3();
        // demo4();
        demo5();
    }

    /**
     * 从给定的对象序列中创建数据流
     * @throws Exception
     */
    public static void demo() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Person> persons = senv.fromElements(
                new Person("张三", 30)
                , new Person("张三", 30)
                , new Person("李四", 40)
                , new Person("李四", 40)
                , new Person("王五", 50)
                , new Person("王五", 50));

        persons.print();
        senv.execute("DataSourceDemo");
    }

    /**
     * 从 Java Java.util.Collection 创建数据流
     * @throws Exception
     */
    public static void demo2() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Person> personList = new ArrayList<>();
        personList.add(new Person("张三", 30));
        personList.add(new Person("张三", 30));
        personList.add(new Person("李四", 40));
        personList.add(new Person("李四", 40));
        personList.add(new Person("王五", 50));
        personList.add(new Person("王五", 50));

        DataStream<Person> persons = senv.fromCollection(personList);

        persons.print();
        senv.execute("DataSourceDemo");
    }

    /**
     * 从 file 创建数据流
     * @throws Exception
     */
    public static void demo3() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 这个文件目录可以是uri，或者hdfs目录
        DataStream<Person> persons = senv.readTextFile("D:/space/IJ/BigDataTechnical/Flink/src/main/java/operator/person.txt")
                .map(item ->
            new Person(item.split(",")[0], Integer.valueOf(item.split(",")[1])));

        persons.print();
        senv.execute("DataSourceDemo");
    }

    /**
     * 从 socket 创建数据流
     * @throws Exception
     */
    public static void demo4() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 1. linux安装nc工具：yum install nc
         * 2. 发送数据： nc -lk 9999
          */
        DataStream<Person> persons = senv.socketTextStream("192.168.20.130", 9999)
                .map(line -> new Person(line.split(",")[0], Integer.valueOf(line.split(",")[1])));

        persons.print();
        senv.execute("DataSourceDemo");
    }

    /**
     * 从 自定义 创建数据流
     * @throws Exception
     */
    public static void demo5() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 为了简单起见这里实现了SourceFunction接口，实际工作中最常用的数据源是那些支持低延迟，
         * 高吞吐并行读取以及重复（高性能和容错能力为先决条件）的数据源，例如 Apache Kafka，Kinesis 和各种文件系统。
         */
        DataStream<Person> persons = senv.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                ctx.collect("张三,30");
            }

            @Override
            public void cancel() {

            }})
                .map(line -> new Person(line.split(",")[0], Integer.valueOf(line.split(",")[1])));

        persons.print();
        senv.execute("DataSourceDemo");
    }


}
