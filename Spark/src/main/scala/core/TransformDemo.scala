package core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: keguang
  * @Date: 2022/1/5 13:45
  * @version: v1.0.0
  * @description: RDD transform 算子
  */
object TransformDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.parallelize(1.to(10))
    // map算子作用于每个元素
    rdd.map(item => item * 2).collect().foreach(println)

    // mapPartitions算子一次处理一个分区
    rdd.mapPartitions(iter => {
      iter.map(item => item * 2)
    }).collect().foreach(println)

    // mapPartitionsWithIndex算子类似于mapPartitions，不过可以获取到分区索引
    rdd.mapPartitionsWithIndex((index, iter) => {
      iter.map(item => (index, item * 2))
    }).collect().foreach(println)

    // flatMap类似于map，将算子扁平处理，一行变多行，类似于sql中的explode
    rdd.flatMap(item => 1.to(item)).collect().foreach(println)

    // 返回RDD[Array[T]]
    println(rdd.glom().map(item => item.max).sum())

    // 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样的操作称之为 shuffle。
    rdd.groupBy(item => item % 2).mapPartitionsWithIndex((index, iter) => {
      iter.map(item => (index, item))
    }).collect().foreach(println)

    // filter 条件过滤
    rdd.filter(item => item % 3 == 1).collect().foreach(println)

    // 抽数据集
    rdd.sample(false, 0.5).collect().foreach(println)

    // 去重
    rdd.distinct().collect().foreach(println)

    // 重分区，默认shuffle=false，不进行shuffle，减小分区是可以的，窄依赖；如果要增加分区，需要shuffle。
    rdd.coalesce(2)

    // 重分区，其实调用的是coalesce(numPartitions, shuffle = true)，即会有shuffle。
    rdd.repartition(2)

    // 按照处理后的结果进行排序，比如这里按照对3取模后进行排序
    rdd.sortBy(item => item % 3).collect().foreach(println)

    // 求交集
    val rdd2 = sparkContext.parallelize(4.to(13))
    rdd.intersection(rdd2).collect().foreach(println)

    // 求并集
    rdd.union(rdd2).collect().foreach(println)

    // 求差集
    rdd.subtract(rdd2).collect().foreach(println)
    rdd2.subtract(rdd).collect().foreach(println)

    // 以(k, v)形式合并键值对
    rdd.zip(rdd2).collect().foreach(println)

    // 按照key进行聚合
    rdd.map(item => (item % 3, 1)).reduceByKey( _ + _).collect().foreach(println)




  }

}
