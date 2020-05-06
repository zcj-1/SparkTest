package IORDDTemplate

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/** @author Chaojay
  * @since 2018-12-20 13:15
  */
// 读取关系型数据库
object JdbcRDDReadTemplate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("jdbcRead").setMaster("local[*]")

    val sc = new SparkContext(conf)
    /**
      * class JdbcRDD[T](sc : org.apache.spark.SparkContext,
      *                  getConnection : scala.Function0[java.sql.Connection], s
      *                  ql : scala.Predef.String,
      *                  lowerBound : scala.Long,
      *                  upperBound : scala.Long,
      *                  numPartitions : scala.Int,
      *                  mapRow : scala.Function1[java.sql.ResultSet, T]
      */
    val rdd = new JdbcRDD(
      sc,
      () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      java.sql.DriverManager.getConnection("jdbc:mysql://Hadoop102:3306/chaojay", "root", "000000")
      },
      "select * from rdd where id >= ? and id <= ?",
      2,
      4,
      1,
      resultSet => (resultSet.getInt(1), resultSet.getString(2), resultSet.getString(3)))

    rdd.foreach(println(_))

    sc.stop()
  }
}
