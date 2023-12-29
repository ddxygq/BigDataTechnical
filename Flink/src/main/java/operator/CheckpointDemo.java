package operator;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckpointDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启checkpoint，并且指定间隔，这里配置为10s进行一次，单位是 ms
        senv.enableCheckpointing(10 * 1000);

        // checkpoint超时时间，单位ms，超过将被丢弃
        senv.getCheckpointConfig().setCheckpointTimeout(5 * 1000);

        // 两个checkpoint的间隔，不是一个完了，下一个立马开始
        senv.getCheckpointConfig().setCheckpointInterval(1000);

        // 配置checkpoint目录
        senv.getCheckpointConfig().setCheckpointStorage("hdfs:///flink/checkpoints/jobname");

        // 设置stateBackend
        senv.setStateBackend(new EmbeddedRocksDBStateBackend());
    }
}
