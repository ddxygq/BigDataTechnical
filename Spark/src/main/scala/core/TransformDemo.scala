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
      iter.map(item => (index, item))
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

    // 将初始值与每个分区作用于第一个函数，然后把结果和初始值带入第二个函数
    /**
      下例中两个函数都做加法。
      每个分区的数据：
      (0,1)
      (1,2)
      (2,3)
      (3,4)
      (3,5)
      (4,6)
      (5,7)
      (6,8)
      (7,9)
      (7,10)

      第一个函数，分区内计算规则：
      先计算每个分区与初始值10的累加值，得：
      0 11
      1 12
      2 13
      3 19
      4 16
      5 17
      6 18
      7 29

      这些分区累加结果：135

      第二个函数，分区间计算规则：
      将初始值10 与 第一个行数的结果做加法，结果是 10 + 135，得145
      */
    println(rdd.aggregate(10)((x, y) => x + y, (x, y) => x + y))

    // 当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为foldByKey
    println(rdd.fold(10)((x, y) => x + y))

    // 对key-value 型 rdd 进行聚集操作，这里计算每个key的平均值，
    // 结果：
    /**
       (0,6.0)
       (1,5.5)
       (2,5.0)
      */
    rdd.map(item => (item % 3, item)).combineByKey(item => (item, 1)
      , (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1)
      ,(acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map(item => (item._1, 1.0 * item._2._1 / item._2._2)).collect().foreach(println)

    // 按照Key排序，默认升序
    rdd.map(item => (item % 3, item)).sortBy(item => item._1).collect().foreach(println)
    rdd.map(item => (item % 3, item)).sortByKey().collect().foreach(println)

    //
    rdd.map(item => (item % 3, item)).join(rdd2.map()).collect().foreach(println)


  }

}
