package wordcountTemplate

import org.apache.spark.{SparkConf, SparkContext}

/** @author Chaojay
  * @since 2018-12-19 16:28
  */
// scala编写的spark的wordcount
object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("wordcountTemplate")

    val sc = new SparkContext(conf)

    // 方式一：按步骤写
    val file = sc.textFile("G:\\testFile\\wordcount\\wordcount.txt")

    val words = file.flatMap(_.split("\t"))

    val wordTunple = words.map((_, 1))

    val result = wordTunple.reduceByKey(_ + _)

    result.foreach(println(_))

    // 方式二：
    val rdd = sc.textFile("G:\\testFile\\wordcount\\wordcount.txt").flatMap(_.split("\t")).map((_, 1)).reduceByKey(_ + _)
    rdd.foreach(println(_))

    sc.stop()
  }
}
