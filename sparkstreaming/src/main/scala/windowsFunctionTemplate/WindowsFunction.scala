package windowsFunctionTemplate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/** @author Chaojay
  * @since 2017-12-25 12:17
  */
// SparkStreaming 的窗口函数
object WindowsFunction extends App {

  val conf = new SparkConf().setAppName("windowsFunction").setMaster("local[*]")

  val ssc = new StreamingContext(conf, Seconds(5))

  val line = ssc.socketTextStream("Hadoop102", 9999)

  val words = line.flatMap(_.split(" "))

  val wordTuple = words.map((_, 1))

  val result = wordTuple.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(15), Seconds(5))
  /**
    * def reduceByKeyAndWindow( reduceFunc: (V, V) => V, windowDuration: Duration， slideDuration: Duration): DStream[(K, V)]

      这个函数是普通版：  reduceFunc  就是用于将窗口内的所有RDD数据进行规约

      def reduceByKeyAndWindow( reduceFunc: (V, V) => V, invReduceFunc: (V, V) => V, windowDuration: Duration, slideDuration: Duration): DStream[(K, V)]

      这个是优化版：  reduceFunc 主要用于将失去的数据“减掉”    invReduceFunc  主要用于将新增的数据“加上”
    */
  result.print()

  ssc.start()
  ssc.awaitTermination()
}
