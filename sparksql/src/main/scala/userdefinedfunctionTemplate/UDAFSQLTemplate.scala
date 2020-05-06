package userdefinedfunctionTemplate

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/** @author Chaojay
  * @since 2018-12-21 17:41
  */
// 自定义 UDAF 函数
/**
  * UDAF 函数分为两种：适用于DSL的强类型，适用于SQL的弱类型
  */
class UDAFSQLTemplate extends UserDefinedAggregateFunction {

  // 输入数据的结构
  override def inputSchema: StructType = {
    StructType(StructField("sum", LongType) :: Nil)
  }

  // 每一个分区中共享变量的结构
  override def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", IntegerType) :: Nil)
  }

  // UDAF函数输出类型
  override def dataType: DataType = {
    DoubleType
  }

  // 如果有相同数据输入是否输出相同结果
  override def deterministic: Boolean = true

  // 每一个分区上的共享变量数据初始化 buffer 就是 StructType 数据
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0
  }

  // 每一个分区上进行数据的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 工资总额度
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    // 工资个数
    buffer(1) = buffer.getInt(1) + 1
  }

  // 合并所有分区上的输出数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  // 产生最终计算结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getInt(1)
  }
}

object UDAFSQLTemplate extends App {

  // 获取 SparkConf 对象
  val conf = new SparkConf().setAppName("udafSQL").setMaster("local[*]")

  // 获取 SparkSession 的对象
  val spark = SparkSession.builder().config(conf).getOrCreate()

  // 创建 DataFrame 对象
  val df = spark.read.json("F:\\workspaces\\scala\\sparktest\\sparksql\\src\\main\\resources\\employees.json")

  // 注册表名
  df.createOrReplaceTempView("employee")

  // 产生 UDAF 对象
  val average = new UDAFSQLTemplate

  // 注册 UDF 函数 【注意： 参数要指明类型】
  spark.udf.register("aver", average)

  // 使用 UDF　函数
  spark.sql("select aver(salary) aver from employee group by name").show()

  // 关闭 SparkSession 入口
  spark.stop()
}
