package baseTemplate

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** @author Chaojay
  * @since 2018-12-26 10:31
  */
object BaseOper extends App{
  /**
    * Spark GraphX 没有基本抽象，是通过SparkContext 来创建的
    */

  val conf = new SparkConf().setAppName("Graphx").setMaster("local[*]")

  val sc = new SparkContext(conf)

  /**
    * 创建顶点的RDD 。顶点包括顶点ID 和 顶点属性
    * def apply[VD: ClassTag](vertices: RDD[(VertexId, VD)]): VertexRDD[VD]
    *                                         -- type VertexId = Long  是 scala.Long 的别名
    *                                         -- VD: ClassTag 顶点的属性值，任意类型
    *  创建顶点类型的RDD即VertexRDD ，参数 ： vertices: RDD[(VertexId, VD)]
    */
  val vertexRDD: RDD[(Long, (String, Int))] = sc.makeRDD(Array((1L, ("Tony", 18)),
                                                                (2L, ("Alice", 22)),
                                                                (3L, ("Sara", 20)),
                                                                (4L, ("Paul", 20)),
                                                                (5L, ("Tess", 19)),
                                                                (6L, ("Neal", 21)),
                                                                (7L, ("Hans", 19)),
                                                                (8L, ("Wendy", 21)),
                                                                (9L, ("Garry", 20))))

  /**
    * 创建边的RDD 。边包括源顶点ID，目的顶点ID，以及边的属性
    *
     */
  val edgeRDD:RDD[Edge[String]] = sc.makeRDD(Array(Edge(1L, 2L, "like"),
                                                Edge(1L, 8L, "like"),
                                                Edge(2L, 7L, "like"),
                                                Edge(3L, 1L, "hate"),
                                                Edge(4L, 8L, "like"),
                                                Edge(5L, 4L, "like"),
                                                Edge(6L, 7L, "like"),
                                                Edge(7l, 3L, "like"),
                                                Edge(8L, 4L, "like"),
                                                Edge(8L, 1L, "hate"),
                                                Edge(9L, 5L, "hate"),
                                                Edge(9L, 2L, "like")))

  /**
    *  创建Graph有三种方式：分别通过调用 apply, fromEdges, fromEdgeTuples 三个方法进行创建
    *   def apply[VD: ClassTag, ED: ClassTag](
                                            vertices: RDD[(VertexId, VD)],
                                            edges: RDD[Edge[ED]],
                                            defaultVertexAttr: VD = null.asInstanceOf[VD],
                                            edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                            vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED]

        下面两种创建方式：顶点属性值都是一样的
        def fromEdges[VD: ClassTag, ED: ClassTag](
                                                  edges: RDD[Edge[ED]],
                                                  defaultValue: VD,
                                                  edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                                  vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED]

        def fromEdgeTuples[VD: ClassTag](  // 这种方式有默认的边属性值为 1
                                        rawEdges: RDD[(VertexId, VertexId)],
                                        defaultValue: VD,
                                        uniqueEdges: Option[PartitionStrategy] = None,
                                        edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                        vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, Int]
    */
  // 通过调用apply方法创建 Graph
  val graph = Graph(vertexRDD, edgeRDD) // 包含顶点ID，属性值，以及边的属性值

  // 通过调用 fromEdges 方法创建 Graph
  val graph1 = Graph.fromEdges(edgeRDD, "abc") // 只有边属性值和顶点属性值，且所有顶点的属性值都是 abc,

  // 通过调用 fromEdgeTuples 方法创建 Graph
  // 先要获取 rawEdges: RDD[(VertexId, VertexId)]
  val rawEdges: RDD[(VertexId, VertexId)] = edgeRDD.map(edge => (edge.srcId, edge.dstId))
  val graph2 = Graph.fromEdgeTuples(rawEdges, "abc") // 只有源顶点的ID,目的顶点的ID，以及顶点属性值，且所有顶点的属性值都是 abc,


  println()
  println("*************************************** 图的属性操作 ***************************************")
  println()

  println("- - - 图 Graph 的所有边：")
  // edges 返回图的边 (srcId, attr, dstId): EdgeRDD[ED]
  graph.edges.foreachPartition(items => for (elem <- items) {println("srcId: " + elem.srcId + "---> edgeAttr: " + elem.attr + " ---> dstId: " + elem.dstId)})
  println()

  println("- - - 图 Graph 的所有顶点：")
  // vertices 返回图的顶点 (vertexId, attr): VertexRDD[VD]
  graph.vertices.foreach(ver => println(s"顶点：${ver._1}，顶点属性：${ver._2}"))
  println()

  println("- - - 图 Graph 的所有顶点的入度：")
  // inDegrees 返回图的所有顶点的入度 (vertexId, num): VertexRDD[VD]
  graph.inDegrees.foreach(vertex => println(s"顶点: ${vertex._1},入度：${vertex._2}"))
  println()

  println("- - - 图 Graph 的所有顶点的出度：")
  // outDegrees 返回图的所有顶点的出度 (vertexId, num): VertexRDD[VD]
  graph.outDegrees.foreach(vertex => println(s"顶点: ${vertex._1},出度：${vertex._2}"))
  println()

  println("- - - 图 Graph 的所有三元组：")
  // triplets 返回一个三元组 EdgeTriplet(): RDD[EdgeTriplet[VD, ED]]
  graph.triplets.foreach(triplet => println(s"srcId: ${triplet.srcId},srcAttr: ${triplet.srcAttr},Attr: ${triplet.attr},dstId: ${triplet.dstId}, dstAttr: ${triplet.dstAttr}"))
  println()

  println()
  println("*************************************** 图的转换操作 ***************************************")
  println()

  println("- - - 图 Graph 的顶点转换操作：")
  // mapVertices 顶点的转换操作。 返回一个新的 Graph
  graph.mapVertices{case (verid, (name, age)) => (verid + 1, (name, age))}.vertices.foreach(ver => println(s"顶点：${ver._1}，顶点属性：${ver._2}"))
  println()

  println("- - - 图 Graph 的边转换操作：")
  // mapEdges 边的转换操作。 返回一个新的 Graph
  graph.mapEdges(edge => edge.attr + "!!!").edges.foreachPartition(items => for (elem <- items) {println("srcId: " + elem.srcId + "---> edgeAttr: " + elem.attr + " ---> dstId: " + elem.dstId)})
  println()

  println("- - - 图 Graph 的三元组转换操作：")
  // mapTriplets 三元组的转换操作。 返回一个新的 Graph
  graph.mapTriplets(triplet => "so " + triplet.attr).triplets.foreach(triplet => println(s"srcId: ${triplet.srcId},srcAttr: ${triplet.srcAttr},Attr: ${triplet.attr},dstId: ${triplet.dstId}, dstAttr: ${triplet.dstAttr}"))
  println()

  println()
  println("*************************************** 图的结构操作 ***************************************")
  println()

  println("- - - 图 Graph 的截取子图：")
  // subgraph 截取一个子图。 返回一个新的 Graph
  val subGraph = graph.subgraph(subGraphEdges => subGraphEdges.srcId > 3, (id, attr) => attr._2 > 19)
  subGraph.triplets.foreach(triplet => println(s"srcId: ${triplet.srcId},srcAttr: ${triplet.srcAttr},Attr: ${triplet.attr},dstId: ${triplet.dstId}, dstAttr: ${triplet.dstAttr}"))
  println()

  println("- - - 子图 subGraph 的所有边：")
  // 子图所有的边
  subGraph.edges.foreachPartition(items => {
    for (elem <- items) {
      println(s"srcId: ${elem.srcId}  attr: ${elem.attr}  dstId: ${elem.dstId}")
    }})
  println()

  println("- - - 子图 subGraph 的所有顶点：")
  // 子图所有的顶点
  subGraph.vertices.foreachPartition(items => {
    for (elem <- items) {
      println(s"vertexId: ${elem._1}  vertexAttr; ${elem._2}")
    }})
  println()

  println("- - - 子图 subGraph 的反转图：")
  // reverse 将原图反转，即将所有边的指向反转。 返回一个新的 Graph
  val reverseGraph = subGraph.reverse
  println("- - - 子图 subGraph 的反转图的所有边：")
  // 反转后的图的边
  reverseGraph.edges.foreachPartition(items => {
    for (elem <- items) {
      println(s"srcId: ${elem.srcId}  attr: ${elem.attr}  dstId: ${elem.dstId}")
    }})
  println()

  println("- - - 子图 subGraph 的反转图的所有顶点：")
  // 反转后的图的顶点
  reverseGraph.vertices.foreachPartition(items => {
    for (elem <- items) {
      println(s"vertexId: ${elem._1}  vertexAttr; ${elem._2}")
    }})
  println()

  println()
  println("*************************************** 图的连接操作 ***************************************")
  println()

  println()
  println("*************************************** 图的聚合操作 ***************************************")
  println()

  println("- - - 指向某顶点的其他顶点：")
  /**
    * collectNeighbors 与顶点有关系的顶点有哪些，返回一个 (srcId,(relativeId,(attr))): VertexRDD[VD]。
    *       其中参数：
    *               -- EdgeDirection,In         指向该顶点的顶点有哪些
    *               -- EdgeDirection,Out        该顶点指向了哪些顶点
    *               -- EdgeDirection.Either     既没有指向该顶点又没有被该顶点指向的顶点
    *               -- EdgeDirection.Both       指向该定点以及该顶点指向的顶点
    */
  graph.collectNeighbors(EdgeDirection.In).foreachPartition(items =>
    for (elem <- items) {
      println(s"顶点：${elem._1}")
      for (ver <- elem._2) {
        println(s"指向顶点 ${elem._1} 的顶点有： ver: ${ver._1} attr: ${ver._2}")
      }})
  println()

  println("- - - aggregateMessages：")
  // aggregateMessages
  graph.aggregateMessages[String](ect => ect.sendToDst(ect.srcAttr._1), _++_).foreachPartition(items => {
    for (elem <- items) {
      println(s"${elem._1} : ${elem._2}")
    }})

}
