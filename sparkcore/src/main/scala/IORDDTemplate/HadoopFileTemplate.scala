package IORDDTemplate

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

/** @author Chaojay
  * @since 2018-12-20 14:31
  */
// hadoopFile
/**
  * Hadoop的输出和输出格式
    1、输入
      1、针对旧的Hadoop API来说
        提供了 hadoopFile 以及 hadoopRDD 来进行hadoop的输入
      2、针对新的Hadoop API来说
        提供了 newApiHadoopFile 以及 newApiHadoopRDD 来进行 hadoop 的输入

    2、输出
      1、针对旧的Hadoop API来说
        提供了saveAsHadoopFile 以及 saveAsHadoopDataSet 来进行 Hadoop 的输出
      2、针对新的Hadoop API来说
        提供了 saveAsNewApiHadoopFile 以及s aveAsNewApiHadoopDataSet 来进行 Hadoop 的输出
  */
object HadoopFileTemplate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("hadoopfile")

    val sc = new SparkContext(conf)

    // 读取
    val rdd = sc.newAPIHadoopFile("hdfs://Hadoop102:9000/README.txt", classOf[TextInputFormat],
      classOf[LongWritable], classOf[Text])

    rdd.foreach(text => println(text._2))

    // 写入
    rdd.saveAsNewAPIHadoopFile("hdfs://Hadoop102:9000/NewAPIHadoopFileOut", classOf[LongWritable], classOf[Text],
      classOf[TextOutputFormat[LongWritable, Text]])
  }
}
