package transformTemplate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** @author Chaojay
  * @since 2017-12-25 10:17
  */
// SparkStreaming 中的状态转换分为 有状态转换 和 无状态转换

/**
  * 无状态转换即为前后两个时间段所处理的结果无关联，没有依赖关系
  */
object NoStateTransform {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("nostate").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val line = ssc.socketTextStream("Hadoop102", 9999)
    val words = line.flatMap(_.split(" "))
    val wordTuple = words.map((_, 1))
    wordTuple.transform(rdd => rdd.reduceByKey(_ + _)).print() // transform 函数中是RDD的操作

    ssc.start()
    ssc.awaitTermination()
  }
}
