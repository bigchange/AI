package com.bigchange.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by C.J.YOU on 2016/3/16.
  */

object SimpleTextClassificationPipeline {

  case class LabeledDocument(id:Long,text:String,label:Double)

  case class Document(id:Long,text:String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("STCP").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // 准备训练文档，这些文档已被标记
    val trainnig  = sc.parallelize(Seq(
      LabeledDocument(0,"层次",1.0),
      LabeledDocument(1," b d",0.0),
      LabeledDocument(2,"spark f g h",0.0),
      LabeledDocument(3,"这种分类",1.0),
      LabeledDocument(4,"kunyandata",1.0)))

    // 配置 ML 的流程，包括三个阶段：Tokenizer（将输入的字符转为小写，并按空格分隔开），hashingTF(用散列法映射到其长期频率)，LogisticRegression（逻辑回归，分类）
    val tokenizer  = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setNumFeatures(10)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    // println(hashingTF.toString())

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)  // 设置正则化参数

    val pipeline  = new Pipeline()
      .setStages(Array(tokenizer,hashingTF,lr))

    // 让流程去训练文档
    val model = pipeline.fit(trainnig.toDF())

    // 读入分词后的文本数据
    /*val  iks = new IKSegmentation(Source.fromFile(new File(args(0))).bufferedReader(),true)
    var lexeme = iks.next()
    val words  = new ListBuffer[String]
    while(lexeme != null){
      words.+=(lexeme.getLexemeText)
      lexeme = iks.next()
    }*/
    // println(words.mkString(" "))

    // Prepare test documents, which are unlabeled.
    val test = sc.parallelize(Seq(
      // Document(4L, words.mkString(" ")),
      Document(5L, "这种分类 层次"),
      Document(6L, "spark hadoop spark"),
      Document(7L, "这种分类")))

    // 让训练的模型 去预测test文档
    model.transform(test.toDF())
      // .show(10)
      .select("id","text","probability","features","prediction")
      .collect()
      .foreach { case Row(id:Long,text:String,prob:Vector,features:Vector,prediction:Double) =>
        println(s"($id,$text) --> prob = $prob,features =$features \n, prediction=$prediction")
    }

    sc.stop()

  }
}
