package broadcastTemplate

/** @author Chaojay
  * @since 2018-12-20 14:34
  */
object BroadCastTemplate {

  /**
    * 广播变量

    1、如果使用本地变量中，不采用广播变量的形式，那么每一个分区中会有一个该变量的Copy

    2、如果使用了广播变量，那么每一个Executor中会有该变量的一次Copy，【一个Executor【JVM进程】中有很多分区】

    3、怎么用？

       1、val broadcastVar = sc.broadcast(Array(1, 2, 3))  通过sc.broadcast 来创建一个广播变量。

       2、broadcastVar.value   通过value方法获取广播变量的内容。

       3、主要用在百兆数据的分发。
    */
}
