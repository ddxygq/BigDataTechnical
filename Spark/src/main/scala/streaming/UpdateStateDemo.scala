package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: keguang
  * @Date: 2022/1/7 17:52
  * @version: v1.0.0
  * @description: 记录历史状态
  */
object UpdateStateDemo {

  def main(args: Array[String]): Unit = {
    //1.初始化 Spark 配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("./checkpoint")

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(previousCount + currentCount)
    }

    // linux 安装 nc工具：yum install -y nc
    val data = ssc.socketTextStream("cdh-001", 9999)
    data.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
