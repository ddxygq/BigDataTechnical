package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: keguang
  * @Date: 2022/1/7 16:55
  * @version: v1.0.0
  * @description:
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    //1.初始化 Spark 配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // linux 安装 nc工具：yum install -y nc
    val data = ssc.socketTextStream("cdh-001", 9999)
    data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
