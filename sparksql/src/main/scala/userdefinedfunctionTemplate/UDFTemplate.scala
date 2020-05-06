package userdefinedfunctionTemplate

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** @author Chaojay
  * @since 2018-12-21 17:22
  */
// 自定义 UDF 函数 (一般都是随时用随时写)
object UDFTemplate extends App {

  // 获取 SparkConf 对象
  val conf = new SparkConf().setAppName("udf").setMaster("local[*]")

  // 获取 SparkSession 的对象
  val spark = SparkSession.builder().config(conf).getOrCreate()

  // 创建 DataFrame 对象
  val df = spark.read.json("F:\\workspaces\\scala\\sparktest\\sparksql\\src\\main\\resources\\employees.json")

  // 注册表名
  df.createOrReplaceTempView("employee")

  // 注册 UDF 函数 【注意： 参数要指明类型】
  spark.udf.register("add", (field: String) => "A:" + field)

  // 使用 UDF　函数
  spark.sql("select add(name) newName from employee").show()

  // 关闭 SparkSession 入口
  spark.stop()
}
