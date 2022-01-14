package sql

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @Author: keguang
  * @Date: 2022/1/14 14:21
  * @version: v1.0.0
  * @description:
  */
object Demo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("sqldemo").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    val sparkContext = sparkSession.sparkContext
    val rdd = sparkContext.makeRDD(List(
      ("keguang", 26),
      ("ikeguang", 27)
    )).map(item => User(item._1, item._2))

    // rdd 转化为 DataFrame
    val df = rdd.toDF()

    // rdd 转化为 DataSet
    val ds = rdd.toDS()

    // DataSet转换为RDD
    val rdd2 = ds.rdd

    // DataFrame与DataSet转换
    df.as[User]
    ds.toDF()

    df.select("name", "age").show()
    // 注册表，SQL语法
    df.createOrReplaceTempView("user")
    sparkSession.sql("select max(age) as age from user").show()

    // UDF，用户自定义函数
    sparkSession.udf.register("lower2uper", (word: String) => word.toUpperCase)
    sparkSession.sql("select lower2uper(name), age from user").show()

    // 读取mysql
    sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      // 要读取的表名
      .option("dbtable", "test")
      .load()
      .show()

    // 写入mysql
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")
    ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/test", "rdd", props)

    // 支持Hive，把hive-site.xml拿过来，放到resources目录下，为了保险起见最好将hdfs-site.xml也拿过来，干脆都拿过来吧
    val spark = SparkSession.builder()
      .appName("sqldemo")
      .master("local[*]")
      // 该参数表示支持Hive
      .enableHiveSupport()
      .getOrCreate()



    sparkSession.stop()





  }

}

case class User(name: String, age: Int)
