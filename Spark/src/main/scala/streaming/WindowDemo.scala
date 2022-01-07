package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * @Author: keguang
  * @Date: 2022/1/7 18:04
  * @version: v1.0.0
  * @description: 窗口操作
  */
object WindowDemo {

  def main(args: Array[String]): Unit = {
    //1.初始化 Spark 配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // linux 安装 nc工具：yum install -y nc
    val data = ssc.socketTextStream("cdh-001", 9999)
    // 滑动窗口，需要指定窗口大小和滑动步长
    data.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Duration(12000), Duration(6000)).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
