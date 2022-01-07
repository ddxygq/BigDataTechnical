package core

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * @Author: keguang
  * @Date: 2022/1/7 14:13
  * @version: v1.0.0
  * @description: RDD分区器: 1). Hash 分区：对于给定的 key，计算其hashCode,并除以分区个数取余
  *              2). Range 分区：将一定范围内的数据映射到一个分区中，尽量保证每个分区数据均匀，而 且分区间有序
  */
object PartitionDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.parallelize(1.to(10))

    rdd.mapPartitionsWithIndex((index, part) => {
      part.map(item => (index, item))
    }).collect().foreach(println)

    val rdd2 = rdd.map(item => (item, item)).partitionBy(new MyPartitioner(3))
      .mapPartitionsWithIndex((index, part) => {
        part.map(item => (index, item))
      })
      .collect().foreach(println)

    /**
      结果：
      (0,(3,3))
      (0,(6,6))
      (0,(9,9))
      (1,(1,1))
      (1,(4,4))
      (1,(7,7))
      (1,(10,10))
      (2,(2,2))
      (2,(5,5))
      (2,(8,8))
      */

  }


  class MyPartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      key.hashCode() % numPartitions
    }
  }
}
