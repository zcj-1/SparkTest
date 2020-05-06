package lowAPIConnKafka

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import scala.collection.JavaConversions._

/** @author Chaojay
  * @since 2017-12-24 14:58
  */
// Kafka 连接池

// 首先，创建一个代理类 KafkaProxy
class KafkaProxy(broker: String){

  // 创建生产者所需要的属性值
  val prop = Map[String, String](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer"
  )

  // 创建一个生产者的客户端。KafkaProducer 是Java类，所以需要导包
  val kafkaClient = new KafkaProducer[String, String](prop)
}

// 然后，创建一个能创建 KafkaProxy 对象的工厂
class KafkaProxyFactory(broker: String) extends BasePooledObjectFactory[KafkaProxy]{

  // 创建一个 KafkaProxy 实例
  override def create(): KafkaProxy = new KafkaProxy(broker)

  // 包装 KafkaProxy 实例
  override def wrap(obj: KafkaProxy): PooledObject[KafkaProxy] = new DefaultPooledObject[KafkaProxy](obj)
}

// 最后，创建类似于单例的Kafka连接池
object KafkaPool {

  private var pool: GenericObjectPool[KafkaProxy] = null

  def apply(broker: String): GenericObjectPool[KafkaProxy] = {
    if (pool == null){
      KafkaPool.synchronized{
        this.pool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(broker))
      }
    }
    this.pool
  }
}
