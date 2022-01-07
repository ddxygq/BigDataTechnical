package core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: keguang
  * @Date: 2022/1/7 15:26
  * @version: v1.0.0
  * @description: 广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个 或多个 Spark 操作使用。
  */
object BroadcastDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.parallelize(1.to(10))

    val broadcast: Broadcast[Int] = sparkContext.broadcast(4)
    rdd.filter(item => item == broadcast.value).foreach(println)

  }

}
