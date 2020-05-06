package userdefinedfunctionTemplate

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator


/** @author Chaojay
  * @since 2018-12-21 18:06
  */
// 适用于 DSL 的强类型 UDAF 【不常用，因为都是用 SQL 操作的，一般不用 DSL（就是调用方法） 操作】

// 创建输入数据的样例类
case class Employee(name: String, salary: Double)

// 创建共享变量的样例类
case class Buff(var sum: Double, var count: Int)

class UDAFDSLTemplate extends Aggregator[Employee, Buff, Double]{

  // 共享变量初始化
  override def zero: Buff = {
    Buff(0.0, 0)
  }

  // 每一个分区上的计算
  override def reduce(b: Buff, a: Employee): Buff = {
    b.sum = b.sum + a.salary
    b.count = b.count + 1
    b
  }

  // 合并所有分区的输出数据
  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // 产生计算结果
  override def finish(reduction: Buff): Double = {
    reduction.sum / reduction.count
  }

  // 对共享变量进行编码
  override def bufferEncoder: Encoder[Buff] = Encoders.product

  // 对输出的数据进行编码
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object UDAFDSLTemplate extends App{

  // 获取 SparkConf 对象
  val conf = new SparkConf().setAppName("udafDSL").setMaster("local[*]")

  // 获取 SparkSession 的对象
  val spark = SparkSession.builder().config(conf).getOrCreate()

  import spark.implicits._

  // 创建 DataSet 对象 【注意：此处必须是一个 DataSet 对象。因为是强类型的】
  val df = spark.read.json("F:\\workspaces\\scala\\sparktest\\sparksql\\src\\main\\resources\\employees.json").as[Employee]

  // 强类型 UDAF 函数的使用
  val aver = new UDAFDSLTemplate().toColumn.name("average")

  // ( 因为查询语句没有使用 group by，所以没有分组，输出的是所有工资的和)
  df.select(aver).show()

  // 关闭 SparkSession 入口
  spark.stop()
}
