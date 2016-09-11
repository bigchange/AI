package com.bigchange.mllib

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by Administrator on 2016/1/14.
  *  spark mllib 中的tf-idf 算法计算文档相似度
  */
object TFIDF {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TfIdfTest")
      .setMaster("local")
    val sc = new SparkContext(conf)

    // Load documents (one per line).要求每行作为一个document,这里zipWithIndex将每一行的行号作为doc id
    val documents = sc.parallelize(Source.fromFile("J:\\github\\dataSet\\TFIDF-DOC").getLines()
      .filter(_.trim.length > 0).toSeq)
      .map(_.split(" ").toSeq)
      .zipWithIndex()


    // feature number
    val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
    //line number for doc id，每一行的分词结果生成tf vector
    val idAndTFVector = documents.map {
      case (seq, num) =>
        val tf = hashingTF.transform(seq)
        (num + 1, tf)
    }
    idAndTFVector.cache()
    // build idf model
    val idf = new IDF().fit(idAndTFVector.values)
    // transform tf vector to tf-idf vector
    val idAndTFIDFVector = idAndTFVector.mapValues(v => idf.transform(v))
    // broadcast tf-idf vectors
    val idAndTFIDFVectorBroadCast = sc.broadcast(idAndTFIDFVector.collect())

    // cal doc cosineSimilarity
    val docSims = idAndTFIDFVector.flatMap {
      case (id1, idf1) =>
        // filter the same doc id
        val idfs = idAndTFIDFVectorBroadCast.value.filter(_._1 != id1)
        val sv1 = idf1.asInstanceOf[SV]
        import breeze.linalg._
        val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
        idfs.map {
          case (id2, idf2) =>
            val sv2 = idf2.asInstanceOf[SV]
            val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
            val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))
            (id1, id2, cosSim)
        }
    }
    docSims.foreach(println)

    sc.stop()

  }
}
