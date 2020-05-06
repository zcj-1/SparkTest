package userdefinedfunctionTemplate

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** @author Chaojay
  * @since 2018-12-21 18:33
  */
// 窗口函数 【很重要】
object OverTemplate extends App {

  /**
    * 开窗函数

      rank（）跳跃排序，有两个第二名时后边跟着的是第四名
      dense_rank() 连续排序，有两个第二名时仍然跟着第三名
      over（）开窗函数：
             在使用聚合函数后，会将多行变成一行，而开窗函数是将一行变成多行；
             并且在使用聚合函数后，如果要显示其他的列必须将列加入到group by中，
             而使用开窗函数后，可以不使用group by，直接将所有信息显示出来。
              开窗函数适用于在每一行的最后一列添加聚合函数的结果。
      常用开窗函数：
         1.为每条数据显示聚合信息.(聚合函数() over())
         2.为每条数据提供分组的聚合函数结果(聚合函数() over(partition by 字段) as 别名)
               --按照字段分组，分组后进行计算
         3.与排名函数一起使用(row number() over(order by 字段) as 别名)
      常用分析函数：（最常用的应该是1.2.3 的排序）
         1、row_number() over(partition by ... order by ...)
         2、rank() over(partition by ... order by ...)
         3、dense_rank() over(partition by ... order by ...)
         4、count() over(partition by ... order by ...)
         5、max() over(partition by ... order by ...)
         6、min() over(partition by ... order by ...)
         7、sum() over(partition by ... order by ...)
         8、avg() over(partition by ... order by ...)
         9、first_value() over(partition by ... order by ...)
         10、last_value() over(partition by ... order by ...)
         11、lag() over(partition by ... order by ...)
         12、lead() over(partition by ... order by ...)
          lag 和lead 可以 获取结果集中，按一定排序所排列的当前行的上下相邻若干offset 的某个行的某个列(不用结果集的自关联）；
          lag ，lead 分别是向前，向后；
          lag 和lead 有三个参数，第一个参数是列名，第二个参数是偏移的offset，第三个参数是 超出记录窗口时的默认值
    */

  val conf = new SparkConf().setAppName("over").setMaster("local[*]")

  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

  case class Score(name: String, clsNum: Int, score: Double)
  val df = spark.sparkContext.makeRDD(Array(Score("a", 1, 90.0),
                                            Score("b", 1, 82.0),
                                            Score("c", 1, 78.0),
                                            Score("e", 2, 67.0),
                                            Score("f", 2, 88.0),
                                            Score("g", 3, 75.0),
                                            Score("h", 3, 86.0),
                                            Score("i", 3, 92.0),
                                            Score("j", 3, 89.0),
                                            Score("k", 4, 94.0),
                                            Score("l", 4, 56.0),
                                            Score("m", 4, 83.0)
                                            )).toDF("name", "clsNum", "score")

  // 需求：使用窗口函数，求每个班级成绩由高到低的排序

  df.createOrReplaceTempView("grade")

  // 使用窗口函数必须要有别名
  spark.sql("select name, clsNum, score, rank() over(partition by clsNum order by score desc) rank from grade").show()

  spark.stop()
}
