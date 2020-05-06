package outTemplate

/** @author Chaojay
  * @since 2017-12-25 12:37
  */
// SparkStreaming 的输出
object OutputTemplate extends App {

  /**
    * print()
           -- 在运行流程序的驱动结点上打印DStream中每一批次数据的最开始10个元素。这用于开发和调试。
              在Python API中，同样的操作叫pprint()。
   
    saveAsTextFiles(prefix, [suffix])
           -- 以text文件形式存储这个DStream的内容。每一批次的存储文件名基于参数中的prefix和suffix。”
              prefix-Time_IN_MS[.suffix]”. 

    saveAsObjectFiles(prefix, [suffix])
           -- 以Java对象序列化的方式将Stream中的数据保存为 SequenceFiles .
              每一批次的存储文件名基于参数中的为"prefix-TIME_IN_MS[.suffix]". 
              Python中目前不可用。

    saveAsHadoopFiles(prefix, [suffix])
           -- 将Stream中的数据保存为 Hadoop files. 每一批次的存储文件名基于参数中的为"prefix-TIME_IN_MS[.suffix]". 
              Python API Python中目前不可用。
    */
}
