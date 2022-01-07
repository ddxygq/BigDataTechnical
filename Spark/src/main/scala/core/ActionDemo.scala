package core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: keguang
  * @Date: 2022/1/7 11:10
  * @version: v1.0.0
  * @description: RDD action 算子
  */
object ActionDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.parallelize(1.to(10))

    // reduce 聚集RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
    println(rdd.reduce((x, y) => x + y))

    println(rdd.count())

    println(rdd.first())

    rdd.take(5).foreach(println)

    println(rdd.aggregate(0)((x, y) => x + y, (x, y) => x + y))

    // 统计每种 key 的个数
    rdd.map(item => (item % 3, item)).countByKey().foreach(println)

    // 保存文件
    // rdd.coalesce(1).saveAsTextFile("rdd.txt")

    // 遍历rdd中的每一个元素
    rdd.foreach(println)




  }

}
