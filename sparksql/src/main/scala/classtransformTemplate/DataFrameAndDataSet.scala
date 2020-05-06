package classtransformTemplate

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** @author Chaojay
  * @since 2018-12-21 12:26
  */
// DataFrame 和 DataSet 之间的相互转换
object DataFrameAndDataSet extends App {

  val conf = new SparkConf().setAppName("test").setMaster("local[*]")

  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._ // 需要引入隐式转换

  // 1、DataFrame 转换 DataSet  【需要借助样例类】
  // 创建样例类
  case class Employee(name: String, salary: BigInt)
  val df = spark.read.json("F:\\workspaces\\scala\\sparktest\\sparksql\\src\\main\\resources\\employees.json")

  val dfToDs = df.as[Employee]

  dfToDs.map(_.name).show()

  // 2、DataSet 转换 DataFrame
  val dsToDf = dfToDs.toDF()

  dsToDf.map(_.getLong(1)).show()

  spark.stop()
}
