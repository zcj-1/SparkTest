package InputTemplate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** @author Chaojay
  * @since 2017-12-23 20:26
  */
// SparkStreaming 的输入源
object TextFileStreamTemplate extends App {

  /**
    * SparkStreaming 的输入
    *             -- 1、ssc.textFileStream("path")  读取文件目录  【基本不用】
    *             -- 2、ssc.socketTextStream(host, port)  读取端口
    *             -- 3、ssc.queueStream(new mutable.SynchronizedQueue[RDD[Int]])  读取RDD队列  【基本不用】
    *             -- 4、ssc.receiverStream(new UserDefinedReceiver)  按照自定义接收器读取数据   【掌握】
    */

  val conf = new SparkConf().setAppName("InputTemplate").setMaster("local[*]")

  val ssc = new StreamingContext(conf, Seconds(5))

  // 1、监控文件目录，读取文件 【文件最好是整体 mv 过来。传输会导致时间过长，而 SparkStreaming 读取是有时间限制的】。基本不用！！！
  val file = ssc.textFileStream("hdfs://Hadoop102:9000/data")

  val words = file.flatMap(_.split(" "))

  val wordTuple = words.map((_, 1))

  val result1 = wordTuple.reduceByKey(_ + _)

  result1.print()

  // 启动
  ssc.start()
  ssc.awaitTermination()
}
