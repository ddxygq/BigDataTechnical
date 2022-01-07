package streaming

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.MysqlPoolUtil

import scala.collection.mutable

/**
  * @Author: keguang
  * @Date: 2022/1/7 17:12
  * @version: v1.0.0
  * @description:
  */
object KafkaDemo {

  def main(args: Array[String]): Unit = {
    //1.初始化 Spark 配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topics = Array("test")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka-001:2181"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> "test-group"
      , "auto.offset.reset" -> "latest"
      , "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 从数据库读取offset信息，json格式：[{"partition":13,"topic":"test-topic","untilOffset":117,"fromOffset":117},{"partition":5,"topic":"test-topic","untilOffset":145,"fromOffset":145},{"partition":15,"topic":"test-topic","untilOffset":154,"fromOffset":154},{"partition":12,"topic":"test-topic","untilOffset":42,"fromOffset":42},{"partition":4,"topic":"test-topic","untilOffset":153,"fromOffset":153},{"partition":16,"topic":"test-topic","untilOffset":157,"fromOffset":156},{"partition":0,"topic":"test-topic","untilOffset":158,"fromOffset":157},{"partition":11,"topic":"test-topic","untilOffset":124,"fromOffset":124},{"partition":2,"topic":"test-topic","untilOffset":124,"fromOffset":124},{"partition":10,"topic":"test-topic","untilOffset":205,"fromOffset":205},{"partition":14,"topic":"test-topic","untilOffset":213,"fromOffset":213},{"partition":6,"topic":"test-topic","untilOffset":153,"fromOffset":153},{"partition":3,"topic":"test-topic","untilOffset":205,"fromOffset":205},{"partition":8,"topic":"test-topic","untilOffset":234,"fromOffset":232},{"partition":7,"topic":"test-topic","untilOffset":208,"fromOffset":208},{"partition":1,"topic":"test-topic","untilOffset":119,"fromOffset":119},{"partition":9,"topic":"test-topic","untilOffset":236,"fromOffset":235}]
    val tpMap = getLastOffsets("test")

    var messages: InputDStream[ConsumerRecord[String, String]] = null
    if (tpMap.nonEmpty) {
      // 非第一次启动程序，从上次保存的offset消费
      messages = KafkaUtils.createDirectStream[String, String](
        ssc
        , LocationStrategies.PreferConsistent
        , ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, tpMap.toMap)
      )
    } else {
      // 首次启动程序，按照auto.offset.reset设置的值，从最新处消费
      messages = KafkaUtils.createDirectStream[String, String](
        ssc
        , LocationStrategies.PreferConsistent
        , ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    }

    messages.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val offset = offsetRanges2Json(offsetRanges).toString

      // TODO

      /**
        统计结果保存数据库，将offset作为mysql表的一个字段，跟统计结果一起保存到数据库，也可以使用hbase、redis等。
        比如：insert result_table value(...., offset)
        */

    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

  /**
    * 从mysql查询offset
    *
    * @param tbName
    * @return
    */
  def getLastOffsets(tbName: String): mutable.HashMap[TopicPartition, Long] = {
    val sql = s"select offset from ${tbName} where id = (select max(id) from ${tbName})"
    val conn = MysqlPoolUtil.getConnection()
    val psts = conn.prepareStatement(sql)
    val res = psts.executeQuery()
    var tpMap: mutable.HashMap[TopicPartition, Long] = mutable.HashMap[TopicPartition, Long]()
    while (res.next()) {
      val o = res.getString(1)
      val jSONArray = JSON.parseArray(o)
      jSONArray.toArray().foreach(offset => {
        val json = JSON.parseObject(offset.toString)
        val topicAndPartition = new TopicPartition(json.getString("topic"), json.getInteger("partition"))
        tpMap.put(topicAndPartition, json.getLong("untilOffset"))
      })
    }
    MysqlPoolUtil.closeCon(res, psts, conn)
    tpMap
  }

  /**
    * 将offset信息格式化为json格式
    * @param arr
    * @return
    */
  def offsetRanges2Json(arr: Array[OffsetRange]): JSONArray = {
    val jSONArray = new JSONArray()
    arr.foreach(offsetRange => {
      val jsonObject = new JSONObject()
      jsonObject.put("partition", offsetRange.partition)
      jsonObject.put("fromOffset", offsetRange.fromOffset)
      jsonObject.put("untilOffset", offsetRange.untilOffset)
      jsonObject.put("topic", offsetRange.topic)

      jSONArray.add(jsonObject)
    })

    jSONArray
  }

}
