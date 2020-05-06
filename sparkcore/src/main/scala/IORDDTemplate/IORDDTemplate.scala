package IORDDTemplate

/** @author Chaojay
  * @since 2018-12-20 15:03
  */
object IORDDTemplate {

  /**
    * 1、文本文件的输入和输出
          1、sc.textFile(path)  文本文件的输入
          2、rdd.saveAsTextFile(path)    文本文件的输出

      2、JSON文件的输入和输出
          1、文本文件的输入和输出，需要在程序中手动进行编码和解码

      3、CSV 逗号分隔  TSV  tab分隔   文本文件的输入和输出

      4、SequenceFile 的输入和输出
             1、val sdata = sc.sequenceFile[Int,String]("hdfs://master01:9000/sdata/p*")
            对于读取还说，需要将kv的类型指定一下。
         2、直接调用rdd.saveAsSquenceFile(path) 来进行保存

      5、ObjectFile的输入和输出
          1、val objrdd:RDD[(Int,String)] = sc.objectFile[(Int,String)]("hdfs://master01:9000/objfile/p*")
          对于读取来说，需要设定kv的类型
          2、直接调用rdd.saveAsObjectFile(path) 来进行保存。
    */
}
