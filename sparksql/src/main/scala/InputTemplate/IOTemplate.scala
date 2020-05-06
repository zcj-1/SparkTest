package InputTemplate

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** @author Chaojay
  * @since 2017-12-21 19:20
  */
// SparkSQL 的读取和写出方式：比较规范
object IOTemplate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("IO").setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    /**
      * SparkSQL的读取和写入都有两种方式
      *  如果不显示指定格式，则默认读取 parquet 格式,默认保存为 parquet 格式
      */
    // 方式一：通过 spark.read
    spark.read.json("path")
    spark.read.text("path")
    spark.read.textFile("path")
    spark.read.csv("path")
    spark.read.orc("path")
    spark.read.parquet("path")
    spark.read.table("path")
    //    spark.read.jdbc("jdbc:mysql://localhost:3306/rdd", "rdd", "root", "00000")

    // 方式二：
    spark.read.format("json").load("path")
    spark.read.format("text").load("path")
    spark.read.format("textFile").load("path")
    spark.read.format("csv").load("path")
    spark.read.format("orc").load("path")
    spark.read.format("parquet").load("path")
    spark.read.format("table").load("path")
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/rdd")
      .option("dbtable", " rddtable10")
      .option("user", "root")
      .option("password", "hive")
      .load()


    // 写入 : 需要通过 DataFrame 或 DataSet 对象来写
    val df = spark.read.text("G:/testFile/json/people.txt")
    // 方式一：
    df.write.json("path")
    df.write.text("path")
    df.write.csv("path")
    df.write.orc("path")
    df.write.parquet("path")
    //    df.write.jdbc("jdbc:mysql://localhost:3306/rdd", "rdd",)

    // 方式二：通过 .option() 设置参数，
    /**
      *  mode ： 保存方式：Overwrite、Append、Ignore、ErrorIfExists
      */
    df.write.format("json").mode("overrite").save()
    df.write.format("text").mode("overrite").save()
    df.write.format("csv").mode("overrite").save()
    df.write.format("orc").mode("overrite").save()
    df.write.format("parquet").mode("overrite").save()
    df.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/rdd")
      .option("dbtable", " rddtable10")
      .option("user", "root")
      .option("password", "hive")
      .mode("overwrite")
      .save()


    spark.stop()
  }
}
