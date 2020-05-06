package InputTemplate

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/** @author Chaojay
  * @since 2017-12-23 20:01
  */
// 自定义 SparkStreaming 的接收器 Receiver ，模拟读取 socket 数据,实现wordcount【 需要继承 Receiver 】
class CustomerReceiverTemplate(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  // SparkStreaming 程序启动时将会调用此方法
  override def onStart(): Unit = {

    // 获取 socket 对象
    val socket = new Socket(host, port)

    // 创建输入变量
    var input = ""

    var reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

    // 读取数据
    input = reader.readLine()

    // 当程序没有停止，且读取到了数据就进入循环
    while (!isStopped() && input != null) {
      // 接收到数据就保存
      store(input)
      // 读取下一条数据
      input = reader.readLine()
    }
  }

  // 程序停止时调用
  override def onStop(): Unit = {

  }
}

object CustomerReceiverTemplate extends App {

  val conf = new SparkConf().setAppName("CustomerReceiverTemplate").setMaster("local[*]")

  val ssc = new StreamingContext(conf, Seconds(5))

  // 自定义接收器 Receiver 的使用：通过 ssc.receiverStream 传入自定义接收器的实例
  val dataDStream = ssc.receiverStream(new CustomerReceiverTemplate("Hadoop102", 9999))

  val words = dataDStream.flatMap(_.split(" "))

  val wordTuple = words.map((_, 1))

  val result = wordTuple.reduceByKey(_ + _)

  result.print()

  // 启动
  ssc.start()
  ssc.awaitTermination()
}
