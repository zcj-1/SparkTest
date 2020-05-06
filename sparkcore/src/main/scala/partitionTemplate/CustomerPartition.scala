package partitionTemplate

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/** @author Chaojay
  * @since 2018-12-20 10:14
  */
// 自定义分区:针对KV形式的RDD
class CustomerPartition(num: Int) extends Partitioner {
  override def numPartitions: Int = {
    num
  }

  override def getPartition(key: Any): Int = {
    key.toString.toInt % num
  }
}

object CustomerPartition extends App {

  val conf = new SparkConf().setAppName("CustomerPartition").setMaster("local[*]")

  val sc = new SparkContext(conf)

  val rdd = sc.makeRDD(1 to 10,1).zipWithIndex() // 通过拉链操作转换成键值对型RDD

  val rdd2 = rdd.mapPartitionsWithIndex((index, items) => Iterator(index + ": [" + items.mkString(" ") + "]"))

  rdd2.foreach(println(_))

  val rdd3 = rdd.partitionBy(new CustomerPartition(4))

  val rdd4 = rdd3.mapPartitionsWithIndex((index, items) => Iterator(index + ": [" + items.mkString(" ") + "]"))

  rdd4.foreach(println(_))

  sc.stop()
}
