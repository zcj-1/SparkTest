package CustomerAccumulator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/** @author Chaojay
  * @since 2018-12-20 16:12
  */
class CustomerAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]]{

  private val _hashAcc = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    _hashAcc.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new CustomerAccumulator
    _hashAcc.synchronized({
      newAcc._hashAcc ++= _hashAcc
    })
    newAcc
  }

  override def reset(): Unit = {
    _hashAcc.clear()
  }

  override def add(v: String): Unit = {
    _hashAcc.get(v) match {
      case None => _hashAcc += ((v, 1))
      case Some(count) => _hashAcc += ((v, count + 1))
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc: AccumulatorV2[String, mutable.HashMap[String, Int]] => {
        for ((k, v) <- acc.value) {
          _hashAcc.get(k) match {
            case None => _hashAcc += ((k, v))
            case Some(count) => _hashAcc += ((k, count + v))
          }
        }
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    _hashAcc
  }
}

object Main extends App{

  val conf = new SparkConf().setMaster("local[*]").setAppName("test")

  val sc = new SparkContext(conf)

  val acc = new CustomerAccumulator

  sc.register(acc)

  val rdd = sc.makeRDD(Array("d", "o", "k", "d"))

  rdd.foreach(acc.add(_))

  for ((k, v) <- acc.value) {
    println(k + " : " + v)
  }

  sc.stop()
}