package classtransformTemplate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/** @author Chaojay
  * @since 2018-12-21 12:12
  */
// RDD 与 DataSet 的相互转换
object RDDAndDataSet extends App {

  val conf = new SparkConf().setAppName("RDDAndDataSet").setMaster("local[*]")

  val sc = new SparkContext(conf)

  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._  // RDD 与 DataFrame 以及 DataSet 的转换必须导入 SparkSession 对象的隐式转换

  val rdd = sc.textFile("G:/testFile/json/people.txt")

  // 1、RDD 转换 DataSet 【注意：需要配合样例类使用】
  // 创建样例类，样例类的构造器参数必须和表中的属性名以及类型一致
  case class People(name: String, age: Int)

  val ds = rdd.map(line => {val par = line.split(",");People(par(0), par(1).trim.toInt)}).toDS()

  ds.show()

  // 2、DataSet 转换 RDD
  val dsRdd: RDD[People] = ds.rdd  // 此时的 dsRdd 里面是 People 对象

  dsRdd.foreach(println(_))

  for (elem <- dsRdd.collect()){
    println(elem.name + ":" + elem.age)  // 可以直接通过属性名的scala版get方法获取属性值
  }

  spark.stop()
  sc.stop()
}
