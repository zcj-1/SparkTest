package InputTemplate

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/** @author Chaojay
  * @since 2017-12-23 20:48
  */
// 监控读取 RDD 队列。基本不用！！！
object RDDSynchronizedQueueTemplate extends App {

  val conf = new SparkConf().setAppName("RDDSynchronizedQueueTemplate").setMaster("local[*]")

  val ssc = new StreamingContext(conf, Seconds(5))

  // 创建一个 RDD 队列
  val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()

  val rddQueueDStram = ssc.queueStream(rddQueue)

  //处理队列中的RDD数据
  val mappedStream = rddQueueDStram.map(x => (x % 10, 1))

  val reducedStream = mappedStream.reduceByKey(_ + _)

  //打印结果
  reducedStream.print()

  //启动计算
  ssc.start()

  // Create and push some RDDs into
  for (i <- 1 to 30) {
    rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
    Thread.sleep(2000)

    //通过程序停止StreamingContext的运行
    //ssc.stop()
  }

  ssc.awaitTermination()
}
