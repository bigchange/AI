package com.bigchange.basic
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.graphx.lib.{ConnectedComponents, PageRank, TriangleCount}
import org.apache.spark.graphx.{GraphLoader, GraphXUtils, PartitionStrategy}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by C.J.YOU on 2016/5/3.
  * 介绍图的基本算法:

PageRank: PageRank 算法用于衡量 graph 中每个 vertex 的重要性，具体可以参见 paper PageRank，GraphX 包括 static 和 dynamic 的 PageRank 实现，static 的 PageRank 运行固定次数的迭代，dynamic 的 PageRank 会直到算法收敛。

Connected Components: 连通图算法对图的每个连通部分进行 label，具体是用连通部分的 lowest-numbered vertex 进行 lable。

Triangle Counting: 三角计算是非常有意思的，它是要解决这种问题，对于一个 vertex，它属于一个 triangle，当且仅当它有 2 个相连的 vertices，且这两个 vertices 有 edge 连接它们。
三角计算需要 edges 是 canonical orientation(srcId < dstId)，以及 graph 采用 Graph.partitionBy 的策略。
  */
object GraphAlgorithms {
  def main(args: Array[String]): Unit = {
    /*if (args.length < 4) {
      System.err.println(
        "Usage: Analytics <taskType> <inputfile> --numEPart=<num_edge_partitions> --partStrategy=<partition_strategy_of_graph> [other options]")
      System.err.println("Supported 'taskType' as follows:")
      System.err.println("  pagerank    Compute PageRank(--output=<output_file> [--tol=<tolerance convergence>, --numIter=<itertation num> must set one]")
      System.err.println("  cc          Compute the connected components of vertices")
      System.err.println("  triangles   Count the number of triangles")
      System.exit(1)
    }

    val taskType = args(0) // 任务类型
    val fname = args(1) // 输入文件

    // 解析 "--propertity=value" 的格式
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }

    val options = Map(optionsList: _*)

    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)

    // 分为多少个 partition
    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }
    // partition 的策略
    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString(_))

    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)

    taskType match {
      case "pagerank" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)
        val numIterOpt = options.remove("numIter").map(_.toInt)
        val outFname = options.remove("output").getOrElse("")

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|             PageRank               |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("GraphAlgorithms-PageRank"))

        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        val pr = (numIterOpt match {
          case Some(numIter) => PageRank.run(graph, numIter)
          case None => PageRank.runUntilConvergence(graph, tol)
        }).vertices.cache()

        println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_ + _))

        if (!outFname.isEmpty) {
          System.out.println("Saving pageranks of pages to " + outFname)
          pr.map { case (id, r) => id + "\t" + r }.saveAsTextFile(outFname)
        }

        sc.stop()

      case "cc" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|      Connected Components          |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("GraphAlgorithms-ConnectedComponents"))
        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()

        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        val cc = ConnectedComponents.run(graph)
        println("Components: " + cc.vertices.map { case (vid, data) => data }.distinct())

        sc.stop()

      case "triangles" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|      Triangle Count                |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("TriangleCount(" + fname + ")"))
        val graph = GraphLoader.edgeListFile(sc, fname,
          canonicalOrientation = true,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel)
          // TriangleCount requires the graph to be partitioned
          .partitionBy(partitionStrategy.getOrElse(RandomVertexCut)).cache()

        val triangles = TriangleCount.run(graph)
        println("Triangles: " + triangles.vertices.map {
          case (vid, data) => data.toLong
        }.reduce(_ + _) / 3)

        sc.stop()

      case _ =>
        println("Invalid task type.")
    }*/
  }

}
