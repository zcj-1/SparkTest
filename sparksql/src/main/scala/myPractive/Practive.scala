package myPractive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** @author Chaojay
  * @since 2018-12-22 9:48
  */
// SparkSQL 实战
/**
  *   需求： 1、统计所有订单中每年的销售单数、销售总额
  *         2、计算所有订单每年最大金额订单的销售额
  *         3、统计每年最畅销货品（哪个货品销售额amount在当年最高，哪个就是最畅销货品）
  */

// 创建三张表的样例类
case class TbDate(dateid: String, years: String, theyear: String, month: String, day: String, weekday: String, week: String, quarter: String, period: String, halfmonth: String)

case class TbStockDetail(ordernumber: String, rownum: String, itemid: String, number: String, price: String, amount: Double)

case class TbStock(ordernumber: String, locationid: String, dateid: String)

object Practive extends App {

  val conf = new SparkConf().setAppName("practive").setMaster("local[*]")

  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

  // 直接使用 spark.read.csv(" ").as[] 的方式更简单
  val rdd1 = spark.sparkContext.textFile("F:\\workspaces\\scala\\sparktest\\sparksql\\doc\\tbDate.txt")

  val rdd2 = spark.sparkContext.textFile("F:\\workspaces\\scala\\sparktest\\sparksql\\doc\\tbStockDetail.txt")

  val rdd3= spark.sparkContext.textFile("F:\\workspaces\\scala\\sparktest\\sparksql\\doc\\tbStock.txt")

  // 创建 tbDate 表。或者不用创建表，直接使用 ds1.write.saveAsTable("tbDate") ,默认保存在 Hive
  val ds1 = rdd1.map(line => {
                                val par = line.split(",")
                                TbDate(
                                  par(0), par(1), par(2), par(3), par(4),
                                  par(5), par(6), par(7), par(8), par(9))
                              }).toDS()

  ds1.createOrReplaceTempView("tbDate")

  // 创建 tbStockDetail 表
  val ds2 = rdd2.map(line => {
                                val par = line.split(",")
                                TbStockDetail(
                                  par(0), par(1), par(2),
                                  par(3), par(4), par(5).trim.toDouble)
                              }).toDS()

  ds2.createOrReplaceTempView("tbStockDetail")

  // 创建 tbStock 表
  val ds3 = rdd3.map(line => {
                                val par = line.split(",")
                                TbStock(par(0), par(1), par(2))
                              }).toDS()

  ds3.createOrReplaceTempView("tbStock")


  // 1、统计所有订单中每年的销售单数、销售总额
  println("====== 1、统计所有订单中每年的销售单数、销售总额 ======")
  spark.sql("SELECT count(DISTINCT sd.ordernumber) num, sum(sd.amount), tmp.theyear FROM tbStockDetail sd, " +
            "(SELECT d.theyear, s.ordernumber FROM tbStock s, tbDate d WHERE s.dateid = d.dateid) tmp " +
            "WHERE sd.ordernumber = tmp.ordernumber GROUP BY tmp.theyear order by tmp.theyear").show()

  // 2、计算所有订单每年最大金额订单的销售额
  println("====== 2、计算所有订单每年最大金额订单的销售额 ======")
  spark.sql("select max(sum) max, tmp3.theyear from (select tmp.ordernumber, sum(sd.amount) sum from tbStockDetail sd, " +
            "(select s.ordernumber, d.theyear from tbStock s, tbDate d where s.dateid = d.dateid) tmp " +
            "where sd.ordernumber = tmp.ordernumber group by tmp.ordernumber) tmp2, " +
            "(select s.ordernumber, d.theyear from tbStock s, tbDate d where s.dateid = d.dateid) tmp3 " +
            "where tmp2.ordernumber = tmp3.ordernumber group by tmp3.theyear order by tmp3.theyear").show()

  // 3、统计每年最畅销货品（哪个货品销售额amount在当年最高，哪个就是最畅销货品）
  println("====== 3、统计每年最畅销货品（哪个货品销售额amount在当年最高，哪个就是最畅销货品）======")
  spark.sql("select tmp2.theyear, max(tmp2.sum) from (select tmp.theyear, sd.itemid, sum(sd.amount) sum " +
            "from tbStockDetail sd, (select s.ordernumber, d.theyear from tbStock s, tbDate d where s.dateid = d.dateid) tmp " +
            "where sd.ordernumber = tmp.ordernumber group by tmp.theyear, sd.itemid) tmp2 group by tmp2.theyear " +
            "order by tmp2.theyear").show()


  spark.stop()
}
