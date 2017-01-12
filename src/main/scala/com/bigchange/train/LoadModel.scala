package com.bigchange.train

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

import com.bigchange.util.{FileUtil}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/9/28.
  * 加载训练好的模型，使用其来预测
  */
object LoadModel {

  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("ORII").setMaster("local"))

  def main(args: Array[String]) {

    val model = NaiveBayesModel.load(sc, "hdfs://61.147.114.85:9000/user/youchaojiang/model_CIFAR-10")

    val data = sc.textFile("file:///F:/SmartData-X/DataSet/CIFAR-10/data/test/*").map(_.split("\t")).map { x =>
      val label = x(0).toDouble
      val vector = x.slice(1, x.length).map(_.toDouble)
      (label, Vectors.dense(vector))
    }

    val  testData = data.map { case (l,v) => LabeledPoint(l, v)}

    test(model, testData)

  }
  // PCA
  def deduceFeatures(tempData: RDD[org.apache.spark.mllib.linalg.Vector]) = {

    val rowMatrix = new RowMatrix(tempData)
    val pca = rowMatrix.computePrincipalComponents(144)
    val reflect = rowMatrix.multiply(pca)

    val deducedFeatures = reflect.rows

    deducedFeatures
  }


  def test(model: NaiveBayesModel, testData:RDD[LabeledPoint]) = {

    val labeledMapReverse = sc.textFile("F:/SmartData-X/DataSet/CIFAR-10/data/batches.meta").map(_.split("\t")).map(x => x(0).toDouble -> x(1)).collectAsMap()
    val predictionAndLabels = testData.map(p => (model.predict(p.features), p.label))
    val result = predictionAndLabels.filter(x => x._1 != x._2).map(x=> (x._1.toLong, x._2.toLong)).map(x => (labeledMapReverse(x._1),labeledMapReverse(x._2))).map(x => x._1 + "\t" + x._2).collect()
    FileUtil.normalFileWriter("E:/github/CIFAR-10",result)
    val metrics = new MulticlassMetrics(predictionAndLabels)
    println("precision:" + metrics.precision)
    println("加权F-指标：" + metrics.weightedFMeasure) // 0.28

  }

  // label 和 feature 分开的时候
  def test(model: NaiveBayesModel, testPixels:RDD[Array[Double]], testLabel: RDD[String], labeledMap: mutable.HashMap[String,Long]) = {

    // 为每张图片创建向量对象
    val testVectors = testPixels.map { x => Vectors.dense(x) }
    testVectors.setName("image-test-vectors") // web 界面方便识别
    testVectors.cache()

    // 正则化
    val testScaler = new StandardScaler(withMean = true, withStd = false).fit(testVectors) // 提取mean
    val testScaledVectors = testVectors.map { v => testScaler.transform(v) } // 向量减去当前列的平均值

    val zippedTest = testLabel.zip(testVectors)

    val test = zippedTest.map {  case (key, vector) => LabeledPoint(labeledMap(key), vector)}

    val predictionAndLabels = test.map(p => (model.predict(p.features), p.label))
    val labeledMapRDD = sc.parallelize(labeledMap.toSeq)

    val labeledMapReverse = labeledMapRDD.map(x =>(x._2, x._1)).collectAsMap()

    val result = predictionAndLabels.filter(x => x._1 != x._2).map(x=> (x._1.toLong, x._2.toLong)).map(x => (labeledMapReverse(x._1),labeledMapReverse(x._2))).map(x => x._1 + "\t" + x._2).collect()

    FileUtil.normalFileWriter("E:/github/CIFAR-10",result)

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println("加权F-指标：" + metrics.weightedFMeasure) // 加权F-指标：0.781142389463205

  }

  // 彩色图片： 三维像素数组或矩阵，（x, y, RGB三元色的值），灰度图片：每个像素只需要一个值表示。
  // pic load: BufferedImage@616893c7: type = 5 ColorModel: #pixelBits = 24 numComponents = 3 color space = java.awt.color.ICC_ColorSpace@18be1fc7 transparency = 1 has alpha = false isAlphaPre = false ByteInterleavedRaster: width = 250 height = 250 #numDataElements 3 dataOff[0] = 2
  def loadImageFromFile(path:String) = ImageIO.read(new File(path))

  // 灰度转变并改变图片尺寸:  颜色组件（#numDataElements 1）
  def processImage(image: BufferedImage, width: Int, height: Int) = {

    val bwImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val g = bwImage.getGraphics
    g.drawImage(image, 0, 0, width, height, null)
    g.dispose()

    bwImage

  }

  // 保存转换后的图片
  def savePic(path: String,image: BufferedImage ) = ImageIO.write(image, "jpg", new File(path + ".jpg"))

  // 提取特征值: 打平二维像素矩阵构造一维的向量
  def getPixelsFromImage(image: BufferedImage) = {

    val width = image.getWidth
    val height = image.getHeight
    val pixels = Array.ofDim[Double](width * height)

    image.getData.getPixels(0, 0, width, height, pixels)

  }

  // 图片处理的组合函数
  def extractPixels(path: String, width: Int, height: Int) = {

    val raw = loadImageFromFile(path)
    val processed = processImage(raw, width, height)

    getPixelsFromImage(processed)

  }


}
