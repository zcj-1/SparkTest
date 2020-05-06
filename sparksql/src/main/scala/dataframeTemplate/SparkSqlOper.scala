package dataframeTemplate

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** @author Chaojay
  * @since 2018-12-21 9:48
  */
/**
  *  SparkSql 的抽象是 DataFrame 和 DataSet 。具有 RDD 的所有特性：不可变，可分区，弹性
  *  Spark1.3 引入 DataFrame，Spark1.6 引入了 DataSet
  *  SparkSql 使用的是off-heap(飞堆，即堆外内存)，还优化了执行计划，采用了谓词下推优化执行
  *  DataFrame 的缺点：编译时不进行类型检查，都是Row对象，运行时会检查。不能部分序列化
  *  DataSet 相比 DataFrame 的优点：泛型时自定义的样例类，编译时会进行类型检查，且支持表中的部分结构进行序列化和反序列化
  */
object SparkSqlOper extends App{

  val conf = new SparkConf().setAppName("sparksql").setMaster("local[*]")

  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._  // DSL 操作需要引入隐式转换

  // sc 可以通过SparkContext创建
  val sc = spark.sparkContext

  // 读取json文件 注意：只能读取一行是一个json的文件
  val sparkJson = spark.read.json("F:\\workspaces\\scala\\sparktest\\sparksql\\src\\main\\resources\\employees.json")

  // 查看df的内容 DataFrame 和 DataSet 都是通过 show 方法打印在控制台
  sparkJson.show()

  // 查看某一列
  // 方式一：
  sparkJson.select("name").show()

  // 方式二：getAs 必须指定泛型 (DSL)
  sparkJson.map(_.getAs[String]("name")).show()

  // 方式三：(DSL)
  sparkJson.map(_.getString(0)).show()

  // 查看表结构
  println(sparkJson.schema)

  // 创建成临时视图，即给表起名
  sparkJson.createOrReplaceTempView("employee")

  /**
    * 注册成临时表的两种方式:
    *     1、sparkJson.createOrReplaceTempView("employee") 常用
    *        特点：① 当前 SparkSession 结束后会自动删除表
    *              ② 直接访问表名，例如：spark.sql("select * from employee")
    *     2、sparkJson.createGlobalTempView("employee")
    *        特点：① 当前SparkContext可用。一个SparkContext可以创建多个SparkSession
    *              ② 访问表需要加上 global_temp. 例如：spark.sql("select * from global_temp.employee")
    */

  spark.sql("select * from employee").show() // 此时可以通过sql语句查询

  spark.stop()
}
