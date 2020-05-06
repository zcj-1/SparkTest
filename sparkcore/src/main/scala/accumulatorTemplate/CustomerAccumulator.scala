package accumulatorTemplate

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/** @author Chaojay
  * @since 2018-12-20 10:30
  */
// 自定义累加器，实现：输入Array("a", "b", "c", "d", "a", "b", "c") 输出 (b,2)，(d,1)，(a,2)，(c,2)
/**
  * 自定义累加器需要继承 AccumulatorV2[in, out] ，
  */
class CustomerAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]]{

  private val _hashAcc = new mutable.HashMap[String, Int]()

  // 检测是否为空
  override def isZero: Boolean = {
    _hashAcc.isEmpty
  }

  // 拷贝一个新的累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new CustomerAccumulator

    _hashAcc.synchronized{
      newAcc._hashAcc ++= _hashAcc
    }
    newAcc
  }

  // 重置累加器
  override def reset(): Unit = {
    _hashAcc.clear()
  }

  // 每一个分区中用于添加数据的方法
  override def add(v: String): Unit = {
    _hashAcc.get(v) match {
      case None => _hashAcc += ((v, 1))
      case Some(count) => _hashAcc += ((v, count + 1))
    }
  }

  // 合并每一个分区的输出
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc: AccumulatorV2[String, mutable.HashMap[String, Int]] =>
        for ((k, v) <- acc.value){
          _hashAcc.get(k) match {
            case None => _hashAcc += ((k, v))
            case Some(count) => _hashAcc += ((k, count + v))
          }
        }
    }
  }

  // 获取累加器的值
  override def value: mutable.HashMap[String, Int] = {
    _hashAcc
  }
}

object CustomerAccumulator extends App{

  val conf = new SparkConf().setAppName("CustomerAccumulator").setMaster("local[*]")

  val sc = new SparkContext(conf)

  val rdd = sc.makeRDD(Array("a", "b", "c", "d", "a", "b", "c"))

  val acc = new CustomerAccumulator

  sc.register(acc) // 累加器对象需要通过SparkContext对象注册

  rdd.foreach(acc.add(_))

  for (elem <- acc.value) {
    print(elem)
  }

  sc.stop()
}
