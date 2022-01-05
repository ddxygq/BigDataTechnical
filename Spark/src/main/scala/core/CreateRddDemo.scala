package core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: keguang
  * @Date: 2022/1/5 11:30
  * @version: v1.0.0
  * @description: 创建RDD的四种方法
  */
object CreateRddDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.parallelize(1.to(10))
    val rdd2 = sparkContext.makeRDD(List(1,2,3,4))
    val rdd3 = sparkContext.textFile("Spark/src/main/resources/rdd.txt")

    rdd.collect().foreach(println)
    rdd2.collect().foreach(println)
    rdd3.collect().foreach(println)

    sparkContext.stop()

  }


}
