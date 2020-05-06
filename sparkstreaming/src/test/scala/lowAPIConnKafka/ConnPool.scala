package lowAPIConnKafka

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import scala.collection.JavaConversions._

/** @author Chaojay
  * @since 2018-12-25 12:43
  */
// 连接池

// 创建代理类
class KafkaProxy(broker: String){

  // 生产者的连接属性
  val proper = Map[String, String](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer"
  )

  // 生产者
  val producer = new KafkaProducer[String, String](proper)
}

// 创建一个能够创建代理类实例的工厂
class KafkaProxyFactory(broker: String) extends BasePooledObjectFactory[KafkaProxy]{
  // 创建代理类实例
  override def create(): KafkaProxy = new KafkaProxy(broker)

  // 包装代理类实例
  override def wrap(obj: KafkaProxy): PooledObject[KafkaProxy] = new DefaultPooledObject[KafkaProxy](obj)
}

// 创建连接池单例
object ConnPool {
  private var pool: GenericObjectPool[KafkaProxy] = null

  def apply(broker: String) = {
    if (pool == null){
      ConnPool.synchronized{
        pool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(broker))
      }
    }
    pool
  }
}
