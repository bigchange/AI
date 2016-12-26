package com.bigchange.basic
import breeze.linalg
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Person(name: String, occupation: String, age: Int)

/**
  * Created by C.J.YOU on 2016/5/3.
  * 该示例展示了图的一些基本用法，包括图的创建，图的一些基本操作等。
  */
object GraphOperation {

  def printGraph[VD, ED](graph: Graph[VD, ED], hint: String): Unit = {
    println(hint)
    graph.triplets.map(triplet => s"<${triplet.srcId},${triplet.srcAttr}> is the ${triplet.attr} of <${triplet.dstId},${triplet.dstAttr}>").collect.foreach(println)
  }

  // Define a reduce operation to compute the highest degree vertex
  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GraphOperation")
    .setMaster("local")
    val ctx = new SparkContext(sparkConf)

    // 我们构造一个真实的图关系，其中包括 “师生” 和 “家庭” 关系，成员角色如下：
    // (enhong.chen, prof)
    // (qi.liu, student)
    // (qifeng.dai, student)
    // (tianwei.dai, girl)
    // (alice, girl) # 这是一个孤立结点

    // 定义 vertex 的 RDD
    val users: RDD[(VertexId, Person)] = ctx.parallelize(Array(
      (1L, Person("enhong.chen", "prof", 40)),
      (2L, Person("qi.liu", "student", 30)),
      (3L, Person("qifeng.dai", "student", 32)),
      (4L, Person("tianwei.dai", "girl", 3)),
      (5L, Person("alice", "girl", 3))
    ))

    // 定义 edge 的 RDD
    val relationships: RDD[Edge[String]] = ctx.parallelize(Array(
      Edge( 1L, 2L, "advisor"),
      Edge(1L, 3L, "advisor"),
      Edge(2L, 3L, "friend"),
      Edge(3L, 2L, "friend"),
      Edge(3L, 4L, "father"),
      Edge(0L, 4L, "mother")
    ))

    // 定义一个默认的 user，当有缺失用户的时候使用
    val defaultUser = Person("None", "Missing", 0)

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    // 找出所有的教授
    graph.vertices.filter { case (id, e) => e.occupation == "prof" }.collect.foreach(println)
    // 找出所有边，其中 srcID > desID 的
    graph.edges.filter { case Edge(src, dst, prop) => src > dst }.collect.foreach(x =>println(x))

    // 测试 triplet 关系
    /**
      * <0,Person(None,Missing,0)> is the mother of <4,Person(tianwei.dai,girl,3)>
      <1,Person(enhong.chen,prof,40)> is the advisor of <2,Person(qi.liu,student,30)>
      <1,Person(enhong.chen,prof,40)> is the advisor of <3,Person(qifeng.dai,student,32)>
      <2,Person(qi.liu,student,30)> is the friend of <3,Person(qifeng.dai,student,32)>
      <3,Person(qifeng.dai,student,32)> is the friend of <2,Person(qi.liu,student,30)>
      <3,Person(qifeng.dai,student,32)> is the father of <4,Person(tianwei.dai,girl,3)>
      */
    printGraph(graph, "===show graph===")

    println("\n|      Property Operators             |\n")

    // 改变某个顶点的 property, 以及相应的关系
    val graph2 = graph.
      mapVertices { case (id, vd) => if (vd.name == "qi.liu") Person(vd.name, "prof", vd.age) else vd }.
      mapEdges(e => if (e.srcId == 1 && e.dstId == 2) "colleague" else e.attr)

    graph2.vertices.filter { case (id, vd) => vd.occupation == "prof" }.collect.foreach(println)

    /**
      * <0,Person(None,Missing,0)> is the mother of <4,Person(tianwei.dai,girl,3)>
        <1,Person(enhong.chen,prof,40)> is the colleague of <2,Person(qi.liu,prof,30)>
        <1,Person(enhong.chen,prof,40)> is the advisor of <3,Person(qifeng.dai,student,32)>
        <2,Person(qi.liu,prof,30)> is the friend of <3,Person(qifeng.dai,student,32)>
        <3,Person(qifeng.dai,student,32)> is the friend of <2,Person(qi.liu,prof,30)>
        <3,Person(qifeng.dai,student,32)> is the father of <4,Person(tianwei.dai,girl,3)>
      */
    printGraph(graph2, "===show graph after alter qi.liu status===")

    println("\n|      Structural Operators             |\n")

    // 改变 graph 的 topoloy
    val ccGraph = graph.connectedComponents()
    val validGraph = graph.subgraph(vpred = (id, attr) => attr.occupation != "Missing") // 获取子图
    val validCCGraph = ccGraph.mask(validGraph)

    printGraph(validCCGraph, "===show connected graph & graph without missing vertex===")

    println("\n|      Join Operators             |\n")

    val u2: RDD[(VertexId, String)] = ctx.parallelize(Array(
      (2L, "prof"),
      (3L, "staff")
    ))

    val joinGraph = graph.joinVertices(u2)((id, v1, v2) => Person(v1.name, v2, v1.age))

    println("===show graph after join===")
    joinGraph.vertices.foreach(println)

    val outerJoinGraph = graph.outerJoinVertices(u2)((id, v1, v2) => (v1.name, v2.getOrElse(v1.occupation)))

    println("===show graph after outer join===")
    outerJoinGraph.vertices.foreach(println)

    println("\n|      Neighborhood Aggregation        |\n")

    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => {
        // Map Function
        if (triplet.srcAttr.age > triplet.dstAttr.age) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr.age)
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )

    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues((id, value) => value match {
        case (count, totalAge) => totalAge / count
      })

    println("===show average of followers===")
    avgAgeOfOlderFollowers.collect.foreach(println(_))

    println("\n|      Computing Degree Information        |\n")

    // Compute the max degrees
    val maxInDegree: (VertexId, Int) = graph.inDegrees.reduce(max)
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    val maxDegrees: (VertexId, Int) = graph.degrees.reduce(max)

    println("===max in-degree===")
    println(maxInDegree)
    println("===max out-degree===")
    println(maxOutDegree)
    println("===max degree===")
    println(maxDegrees)

    println("\n|      Collecting Neighbors       |\n")

    println("===collect neighbors in===")
    graph.collectNeighbors(EdgeDirection.In).collect.foreach { case (id, neighboors) =>
      val s = for (nei <- neighboors) yield nei._1
      println(s"$id -- ${s.mkString(", ")}")
    }

    println("===collect neighbors out===")
    graph.collectNeighbors(EdgeDirection.Out).collect.foreach { case (id, neighboors) =>
      val s = for (nei <- neighboors) yield nei._1
      println(s"$id -- ${s.mkString(", ")}")
    }

    println("===collect neighbors either===")
    graph.collectNeighbors(EdgeDirection.Either).collect.foreach { case (id, neighboors) =>
      val s = for (nei <- neighboors) yield nei._1
      println(s"$id -- ${s.mkString(", ")}")
    }

    println("\n|      Pregel API       |\n")

    val relationships2: RDD[Edge[Int]] = ctx.parallelize(Array(
      Edge(1L, 2L, 3),
      Edge(1L, 3L, 3),
      Edge(2L, 3L, 2),
      Edge(2L, 4L, 50),
      Edge(2L, 5L, 50),
      Edge(3L, 2L, 2),
      Edge(3L, 4L, 1),
      Edge(3L, 5L, 4),
      Edge(0L, 4L, 1)
    ))

    val graph3 = Graph(users, relationships2, defaultUser)

    val sourceId: VertexId = 2 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph3.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => {
        linalg.min(dist, newDist)
      }, // Vertex Program
      triplet => {
        // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )

    println(sssp.vertices.collect.mkString("\n"))

    val setA: VertexRDD[Int] = VertexRDD(ctx.parallelize(0L until 10L).map(id => (id, 1)))
    val rddB: RDD[(VertexId, Double)] = ctx.parallelize(3L until 12L).flatMap(id => List((id, 3.0), (id, 2.0)))

    println("\n|      VertexRDDs       |\n")

    // There should be 20 entries in rddB
    println(rddB.count)

    val setB: VertexRDD[Double] = setA.aggregateUsingIndex(rddB, _ + _)

    // There should be 10 entries in setB
    println(setB.count)

    setB.collect.foreach(println)

    ctx.stop()
  }

}
