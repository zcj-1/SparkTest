package lowAPIConnKafka

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.javaapi
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** @author Chaojay
  * @since 2017-12-24 13:40
  */
// 采用 Kafka 低级API消费
object ConsumerFromKafka extends App {

  val conf = new SparkConf().setAppName("lowAPI").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(5))

  // 创建 topic 信息
  val fromKafka = "from1"
  val toKafka = "to1"

  // 创建 broker 信息
  val brokers = "Hadoop102:9092,Hadoop103:9092,Hadoop104:9092"

  // Kafka 的配置信息
  val propertites = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    // 用于标识消费者所属的消费者组
    ConsumerConfig.GROUP_ID_CONFIG -> "kafka",
    // 如果没有初始化偏移量，或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
  )

  // 获取 Zookeeper 信息
  val zookeeper = "Hadoop102:2181,Hadoop103:2181,Hadoop104:2181"

  // 获取保存 offset 的zookeeper路径
  val topicDirs = new ZKGroupTopicDirs("kafka", fromKafka)
  val zkTopicPath = s"${topicDirs.consumerOffsetDir}" // topicDirs.consumerOffsetDir 即为：/consumers/offsets/topic

  // 创建一个zookeeper连接
  val zkClient = new ZkClient(zookeeper)

  // 获取偏移量保存地址目录下的子节点，即分区数
  val children = zkClient.countChildren(zkTopicPath)

  var kafkaDStream: InputDStream[(String, String)] = null

  if (children > 0) {
    // 新建一个变量，保存消费的偏移量
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    // 首先,需要获取每一个Partition的主节点的信息
    // 创建一个topic的集合
    val topicList = List(fromKafka)
    // 创建一个获取元信息的请求。传入topic集合和相关ID
    val request = new TopicMetadataRequest(topicList, 0)
    // 创建一个连接到 Kafka 消费者的客户端
    val getLeaderConsumer = new SimpleConsumer("Hadoop102", 9092, 100000, 10000, "OffsetLookUp")

    val response = getLeaderConsumer.send(request)

    val topicMetaOption = response.topicsMetadata.headOption

    val partitions = topicMetaOption match {
      case Some(tm) => {
        tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String] // 转成Map类型
      }
      case None => Map[Int, String]()
    }
    getLeaderConsumer.close()

    println("partitions information is :" + partitions)
    println("children information is :" + children)

    for (i <- 0 until children) {

      // 获取保存在zookeeper中的偏移量信息
      val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
      println(s"Partition【${i}】 目前保存的偏移量信息是：${partitionOffset}")

      val tp = TopicAndPartition(fromKafka, i)
      // 获取当前Partition的最小偏移值，【主要为了防止Kafka中的数据过期】
      val requesMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
      val consumerMin = new SimpleConsumer(partitions(i), 9092, 100000, 10000, "getMinOffset")
      val response = consumerMin.getOffsetsBefore(requesMin)

      // 获取partition当前的偏移量
      val currOffsets = response.partitionErrorAndOffsets(tp).offsets
      consumerMin.close()

      // zookeeper中保存的偏移量
      var nextOffset = partitionOffset.toLong

      if (currOffsets.length > 0 && nextOffset < currOffsets.head) {
        nextOffset = currOffsets.head
      }

      println(s"Partition 【${i}】 当前的最小偏移量是：${currOffsets.head}")
      println(s"Partition 【${i}】 修正后的偏移量是：${nextOffset}")

      // 保存偏移量
      fromOffsets += (tp -> nextOffset)
    }

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    println("从zookeeper中获取偏移量创建DStream")
    zkClient.close()
    kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, propertites, Set(fromKafka))
  } else {
    // 说明没有偏移量保存，可以直接创建kafka的DStream对象
    println("没有从zookeeper中获取偏移量")
    kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, propertites, Set(fromKafka))

  }

  var offsetRanges = Array[OffsetRange]()

  // 获取采集的数据的偏移量
  val mapDStream = kafkaDStream.transform { rdd =>
    offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    rdd
  }.map(_._2)

  mapDStream.map(value => "result:" + value)
    .foreachRDD { rdd =>
      rdd
        .foreachPartition { items =>

          // 获取 KafkaPool 连接池对象
          val pool = KafkaPool(brokers)
          // 通过连接池对象获取 KafkaProxy 对象
          val proxy = pool.borrowObject()
          for (elem <- items) {
            proxy.kafkaClient.send(new ProducerRecord(toKafka, elem))
          }

          // 归还 KafkaProxy 对象
          pool.returnObject(proxy)
        }
      // 更新 offset
      val updateTopicDirs = new ZKGroupTopicDirs("kafkaxy", fromKafka)
      val updateZkClient = new ZkClient(zookeeper)
      for (offset <- offsetRanges) {
        println(offset)
        val zkPath = s"${topicDirs.consumerOffsetDir}/${offset.partition}"
        ZkUtils.updatePersistentPath(updateZkClient, zkPath, offset.fromOffset.toString)
      }
      updateZkClient.close()
    }


  // 启动
  ssc.start()
  ssc.awaitTermination()
}
