package IORDDTemplate

import org.apache.spark.{SparkConf, SparkContext}

/** @author Chaojay
  * @since 2018-12-20 13:51
  */
// 写入关系型数据库
object JdbcRDDWriterTemplate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("jdbcWriter")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(Array(6, "Bill", "male"), Array(7, "Rick", "male")))

    rdd.foreachPartition(insertData)

    /**
      * 1、没有提供比较方便的插入数据的方式。
        2、一般直接通过JDBC来完成数据的插入
        3、一般使用 foreachPartition 来批量插入
            -- 1、需要考虑连接池的复用问题。
            -- 2、应该批量插入，提高效率。
      */
  }

  def insertData(iterator: Iterator[Array[Any]]) = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://Hadoop102:3306/chaojay", "root", "000000")

    iterator.foreach(
      data => {
        val ps = conn.prepareStatement("insert into rdd values(?, ?, ?)")
        ps.setInt(1, data(0).toString.toInt)
        ps.setString(2, data(1).toString)
        ps.setString(3, data(2).toString)
        ps.executeUpdate()
      })
  }
}
