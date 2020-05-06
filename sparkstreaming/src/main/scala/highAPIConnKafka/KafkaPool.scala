package highAPIConnKafka

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import scala.collection.JavaConversions._


/** @author Chaojay
  * @since 2017-12-24 11:04
  */
// Kafka 连接池

// 首先，需要创建一个 KafkaProxy 类, 包装 KafkaClient
class KafkaProxy(broker: String){

  val proper = Map[String, String](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer"
  )

  // KafkaProducer 是一个 Java 类，Scala 转 Java 需要导包 ：scala.collection.JavaConversions._
  val kafkaClient = new KafkaProducer[String, String](proper)

}

// 然后，需要创建一个能够创建 KafkaProxy 的工厂
class KafkaProxyFactory(broker: String) extends BasePooledObjectFactory[KafkaProxy]{
  // 创建 KafkaProxy 实例
  override def create(): KafkaProxy = new KafkaProxy(broker)

  // 包装 KafkaProxy 实例
  override def wrap(obj: KafkaProxy): PooledObject[KafkaProxy] = new DefaultPooledObject[KafkaProxy](obj)
}

// 最后，创建类似于单例的连接池
object KafkaPool {

  private var kafkaPool: GenericObjectPool[KafkaProxy] = null

  def apply(broker: String): GenericObjectPool[KafkaProxy] = {

    if (kafkaPool == null){
      // KafkaPool.synchronized 此处要用 KafkaPool 类，而不是 kafkaPool 变量
      KafkaPool.synchronized(
        this.kafkaPool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(broker))
      )
    }
    kafkaPool
  }
}
