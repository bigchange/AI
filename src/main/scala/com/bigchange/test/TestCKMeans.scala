package com.bigchange.test

import com.bigchange.datamining.CKMeans
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by C.J.YOU on 2016/9/21.
  */
object TestCKMeans {

  def main(args: Array[String]) {

    // load data
    val seq = Source.fromFile("src/main/resources/emailConnect/enrondata").getLines().map(_.split(",")).map { x => (x(0),x.slice(1,x.length).map(_.toDouble).filter(y => y != x.slice(1,x.length).map(_.toDouble).max).toList.toArray)}

    val rdd = SparkContext.getOrCreate(new SparkConf().setMaster("local").setAppName("Ckmeans")).parallelize(seq.toSeq)
    //  numClusters:3,numIterator:14,numRuns:4,2.3609887285294123E7 min
    val numCluster = 3 to 3
    val numIterator = 14 to 14
    val numRuns = 4 to 4

    val listBuffer = new ListBuffer[(String, Double)]

    for(c <- numCluster; i <- numIterator; r <- numRuns) {

      val model = CKMeans.model(c, i, r, rdd)
      val result = CKMeans.kmeanPredict(model, rdd)
      val parm = s"numClusters:$c,numIterator:$i,numRuns:$r"
      listBuffer.+=((parm, result))
      // FileUtil.writeToFile("E:\\github\\test", Array(parm,result + "","------\n"))

    }

    listBuffer.sortBy(_._2).reverse.take(5).foreach(println)


  }

}
