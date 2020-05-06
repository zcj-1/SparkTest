package highAPIConnKafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** @author Chaojay
  * @since 2017-12-24 10:35
  */
// 通过 Kafka 高级API连接 SparkStreaming .模拟从 Kafka 中实时读取数据，然后将处理后的数据再发送给 Kafka
object ConnKaka extends App{

  val conf = new SparkConf().setAppName("fromKafka").setMaster("local[*]")

  val ssc = new StreamingContext(conf, Seconds(5))

  // 创建 kafka 的 topic 信息
  val fromKafka = "test"
  val toKafka = "test1"

  // kafka集群 端口是 9092
  val brokers = "hadoop1.macro.com:9092, hadoop2.macro.com:9092, hadoop3.macro.com:9092"

  val kafkaPro = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ConsumerConfig.GROUP_ID_CONFIG -> "kafka",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
  )

  // 创建实时读取kafka数据的 DStream 对象 ,连接到 Kafka.  @return DStream of (Kafka message key, Kafka message value)
  val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder,StringDecoder](ssc, kafkaPro, Set(fromKafka))

  // 逻辑处理  key 什么都没有
  kafkaDStream.map{case (key, value) => value}
    .foreachRDD(rdd =>
      rdd.foreachPartition { items =>

        // 将处理后的数据写回 Kafka ，用到连接池
        val kafkaPool = KafkaPool(brokers)
        // 获取 KafkaProxy 对象
        val kafkaProxy = kafkaPool.borrowObject()

        // 使用
        for (value <- items) {
          kafkaProxy.kafkaClient.send(new ProducerRecord(toKafka, value))
        }

        // 使用完之后归还对象
        kafkaPool.returnObject(kafkaProxy)
      })

  // 启动
  ssc.start()
  ssc.awaitTermination()
}
