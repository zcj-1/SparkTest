package lowAPIConnKafka

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
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
  * @since 2018-12-25 13:01
  */
// 使用Kafka低级API，Offset保存到zookeeper
object LowAPIConsumeKafka extends App {

  val conf = new SparkConf().setAppName("lowAPI").setMaster("local[*]")

  val ssc = new StreamingContext(conf, Seconds(5))

  // kafka集群信息
  val brokers = "Hadoop102:9092,Hadoop103:9092,Hadoop104:9092"

  // 消费的topic
  val getTopic = "from2"
  // 数据处理完成后发送给Kafka的topic
  val toTopic = "to2"

  // 获取Kafka消费者的连接属性
  val proper = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ConsumerConfig.GROUP_ID_CONFIG -> "test",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
  )

  // 获取zookeeper集群信息
  val zkCluster = "Hadoop102:2181,Hadoop103:2181,Hadoop104:2181"

  // 获取zookeeper的客户端连接
  val zkClient = new ZkClient(zkCluster)

  // 获取消费者组 test 消费的 topic 的 offset 在 zookeeper 的存储路径
  val topicOnZKPath = new ZKGroupTopicDirs("test", getTopic)
  val offsetOnZKPath = s"${topicOnZKPath.consumerOffsetDir}"

  // 查看zookeeper中offset路径下的分区数
  val children = zkClient.countChildren(offsetOnZKPath)


  var kafkaDStream: InputDStream[(String, String)] = null
  // 判断：如果offset下的分区数为0，则表示没有存过偏移量，否则即为有偏移量
  if (children == 0) {
    // 此时可以直接消费kafka中的数据，创建DStream
    kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, proper, Set(getTopic))
  } else if (children > 0) {
    // 此时应该从zookeeper中保存的偏移量之后开始读

    // 先定义一个集合用来保存偏移量信息
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    // 创建一个topic集合,用于获取topic的元数据信息
    val topics = Array(getTopic)

    // 创建一个请求，用来获取topic的元数据信息
    val request = new TopicMetadataRequest(topics, 0)

    // 通过连接 Kafka的消费者客户端的形式，发送给客户端请求
    val consumerClient = new SimpleConsumer("Hadoop102", 9092, 30000, 10000, "requestClient")

    // 发送请求，获取应答
    val reponse = consumerClient.send(request)

    // 获取 topic 的元数据信息
    val topicMetadata = reponse.topicsMetadata.headOption

    // 获取分区信息,主要是分区号以及该分区的leader
    val parData = topicMetadata match {
      case Some(topicData) => topicData.partitionsMetadata.map(par => (par.partitionId, par.leader.get.host)).toMap[Int, String]
      case None => Map[Int, String]()
    }
    // 关闭消费者客户端
    consumerClient.close()

    println("分区个数：" + children)
    println("分区号以及分区leader：" + parData)


    /**
      *  为了方式获取数据过期问题，需要将zookeeper中保存的偏移量与当前partition中的偏移量的最小值进行比较，
      *  如果zookeeper保存的偏移量小于当前partition中的偏移量的最小值，那么数据会有过期数据产生，
      *       此时应修正zookeeper中的偏移量等于当前partition中偏移量的最小值
      */
    // 遍历分区，分别求取个分区当前的偏移值
    for (i <- 0 until children){
      // 获取保存在zookeeper中的偏移量
      var offsetInZK = zkClient.readData[String](s"${topicOnZKPath.consumerOffsetDir}/${i}").toLong// 返回的是String，转为Long
      println(s"zookeeper中保存的分区【${i}】的偏移量是: ${offsetInZK}")

      // 获取当前partition中的偏移量的最小值
      val currParMinRequest = OffsetRequest(Map[TopicAndPartition, PartitionOffsetRequestInfo](TopicAndPartition(getTopic, i) -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))

      // 连接Kafka的消费者客户端，向其发送请求获取以获取当前partition的最小偏移值. ！！！这个时候传入的 host 是动态的，所以是 parData(i)
      val getMinConsumerClient = new SimpleConsumer(parData(i), 9092, 30000, 10000, "getMinOffset")
      val getMinReponse = getMinConsumerClient.getOffsetsBefore(currParMinRequest)
      val parMinOffset = getMinReponse.partitionErrorAndOffsets(TopicAndPartition(getTopic, i)).offsets // 返回的是一个seq 所以要使用head方法，返回Long

      // 关闭客户端资源
      getMinConsumerClient.close()

      // 判断两个偏移量的大小
      if (parMinOffset.head > 0 && parMinOffset.head > offsetInZK){
        offsetInZK = parMinOffset.head
      }

      println(s"分区【${i}】中的当前的偏移量的最小值是: ${parMinOffset.head}")
      println(s"修正后的zookeeper中的分区【${i}】的偏移量是: ${offsetInZK}")

      // 保存偏移量
      fromOffsets += (TopicAndPartition(getTopic, i) -> offsetInZK)
    }

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    zkClient.close()

    // 创建读取kafka数据的DStream
    kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, proper, Set(getTopic))
  }

  // 定义一个集合来保存offset
  var offsetRanges = Array[OffsetRange]()

  // 逻辑处理：将从 Kafka 中消费的数据进行处理
  kafkaDStream.transform(rdd => {
    offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    rdd
  }).map(_._2)
    .foreachRDD(rdd => rdd.foreachPartition(items => {  // foreachRDD 中，所有操作都是RDD的操作，和 transform 一样
    // 将处理后的数据写回Kafka
    // 获取连接池对象
    val kafkaPool = ConnPool(brokers)
    // 获取kafka代理
    val kafkaProxy = kafkaPool.borrowObject()

    // 写回
    for (value <- items) {
      kafkaProxy.producer.send(new ProducerRecord[String, String](toTopic, value))
    }

    // 交还代理
    kafkaPool.returnObject(kafkaProxy)

    // 更新保存的偏移量
    val saveZKClient = new ZkClient(zkCluster)
    for (offset <- offsetRanges) {
      println(offset)
      val offsetPath = s"${topicOnZKPath.consumerOffsetDir}/${offset.partition}"
      ZkUtils.updatePersistentPath(saveZKClient, offsetPath, offset.fromOffset.toString)
    }
    saveZKClient.close()
  }))

  ssc.start()
  ssc.awaitTermination()
}
