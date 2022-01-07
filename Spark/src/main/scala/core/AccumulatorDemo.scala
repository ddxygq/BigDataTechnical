package core

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: keguang
  * @Date: 2022/1/7 15:06
  * @version: v1.0.0
  * @description: 累加器用来把Executor 端变量信息聚合到Driver 端。在Driver 程序中定义的变量，在 Executor 端的每个Task 都会得到这个变量的一份新的副本，每个 task 更新这些副本的值后， 传回Driver 端进行 merge。
  */
object AccumulatorDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.parallelize(1.to(10))

    // 系统累加器
    val accu = sparkContext.longAccumulator("accu")

    rdd.foreach(item => accu.add(item))

    println(accu.value)

    // 自定义累加器
    val accu2 = new WordConcatAccumulator
    sparkContext.register(accu2, "accu2")
    rdd.foreach(item => accu2.add(item))
    println(accu2.value)
  }

  class WordConcatAccumulator extends AccumulatorV2[Long, String] {
    var s = ""

    override def isZero: Boolean = s.isEmpty

    override def copy(): AccumulatorV2[Long, String] = new WordConcatAccumulator

    override def reset(): Unit = s = ""

    override def add(v: Long): Unit = s = s + v

    override def merge(other: AccumulatorV2[Long, String]): Unit = {
      s = s + other.value
    }

    override def value: String = s
  }

}
