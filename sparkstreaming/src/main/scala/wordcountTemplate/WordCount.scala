package wordcountTemplate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** @author Chaojay
  * @since 2017-12-23 12:32
  */
// 实时处理来自Hadoop102的9999端口发送的单词，执行wordcount处理。【要先打开发送端口，否则程序报错】
object WordCount extends App {

  // 创建 SparkConf 配置 【注意:因为 SparkStraming 读取数据时会运行一个 receiver 线程，所以此时必须要用 local[*],不能用 local】
  val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

  // 创建 SparkStreaming 的入口对象 StreamingContext
  val ssc = new StreamingContext(conf, Seconds(5))

  val dSteam = ssc.socketTextStream("Hadoop102", 9999)  // 读取 Hadoop102 服务器的 9999 端口数据

  val lineDStream = dSteam.flatMap(_.split(" "))

  val word2DSteam = lineDStream.map((_, 1))

  val wordcount = word2DSteam.reduceByKey(_ + _)

  wordcount.print()  // DStream 在控制台展示是 print 或 println

  // 启动
  ssc.start()
  ssc.awaitTermination()
}
