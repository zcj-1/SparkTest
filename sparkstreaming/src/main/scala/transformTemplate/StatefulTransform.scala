package transformTemplate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/** @author Chaojay
  * @since 2017-12-25 10:29
  */
// 有状态转换：每一次所处理的结果都对上一次的结果有依赖。需要设置CheckPoint目录，用来保存状态信息
object StatefulTransform {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("stateful").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // 设置CheckPoint目录需要通过SparkContext对象
    ssc.sparkContext.setCheckpointDir("./checkPoint")

    val line = ssc.socketTextStream("Hadoop102", 9999)
    val words = line.flatMap(_.split(" "))
    val wordTuple = words.map((_, 1))
    // 有状态转换 通过 updateStateByKey 的方式：
//    wordTuple.updateStateByKey ((value: Seq[Int], state: Option[Int]) => {
//      state match {
//        case None => Some(value.sum)
//        case Some(sum) => Some(sum + value.sum)
//      }}).print()

    // 通过 mapWithState 的方式：
    val myMap = (word: String, value: Option[Int],state: State[Int]) => {
      val sum = state.getOption() match {
        case None => value.sum
        case Some(count) => count + value.sum
      }
      val out = (word, sum)
      state.update(sum)
      out
    }
    wordTuple.mapWithState(StateSpec.function(myMap)).print()


    ssc.start()
    ssc.awaitTermination()
  }
}
