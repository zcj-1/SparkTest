package classtransformTemplate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/** @author Chaojay
  * @since 2018-12-21 11:20
  */
// RDD 与 DataFrame 之间的相互转换
object RDDAndDataFrame extends App {

  val conf = new SparkConf().setAppName("RDDAndDataFrame").setMaster("local[*]")

  val sc = new SparkContext(conf)

  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._  // RDD 与 DataFrame 以及 DataSet 的转换必须导入 SparkSession 对象的隐式转换

  val rdd = sc.textFile("G:/testFile/json/people.txt")

  // 一、RDD 转换 DataFrame
  // 方式一：直接手动转换，常用
  val df = rdd.map(line => {val para = line.split(",");(para(0), para(1).trim.toInt)}).toDF("name", "age")

  df.show()

//  case class Employees(name: String, age: Int)
//  df.as[Employees].show()

  // 方式二：通过反射。此时需要借助样例类
  // 创建样例类
  case class People(name:String, age: Int)

  val df1 = rdd.map(line => {val para = line.split(",");People(para(0), para(1).trim.toInt)}).toDS()

  df1.show()

  // 方式三：通过编程方式转换。该方式会动态添加字段属性（在不知道字段数量以及字段属性时使用）
  // DataFrame 可以认为是有属性结构的 RDD，即可以认为：DataFrame = RDD + Schema。所以，先创建 Schema
  val schema = StructType(StructField("name", StringType) :: StructField("age", IntegerType) :: Nil)

  // 准备 RDD[Row] 。必须是 Row 类型
  val rowRdd = rdd.map(line => {val para = line.split(",");Row(para(0), para(1).trim.toInt)})

  val df2 = spark.createDataFrame(rowRdd, schema)

  df2.show()

  // 二、DataFrame 转换 RDD
  val dfRdd: RDD[Row] = df.rdd  // 此时的 dfRdd 里面是 Row 对象
  for (row <- dfRdd.collect()) {
    println(row.getString(0) + ":" + row.getInt(1))
  }

  spark.stop()
  sc.stop()
}
