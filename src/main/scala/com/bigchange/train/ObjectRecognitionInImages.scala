package com.bigchange.train

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

import com.bigchange.util.FileUtil
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/9/28.
  * Kaggle 赛题中的图片处理：https://www.kaggle.com/c/cifar-10/leaderboard
  */
object ObjectRecognitionInImages {

    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("ORII").setMaster("local"))

    def main(args: Array[String]) {

     /* new File("E:\\github\\lfw-test").listFiles().foreach { x =>
        x.listFiles().foreach { file =>
          val n = file.getAbsolutePath.replace("\\","/").split("/").takeRight(1).head
          file.renameTo(new File("E:/github/lfw-testfile/" + n))
        }
      }
      println("over,<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")*/

      // 允许一次性操作整个文件，不同于之前的在一个文件或多个文件中只能逐行处理
      val rdd = sc.wholeTextFiles("file:///E:/github/lfw-train/*").map { case (fileName, content) => fileName.replace("file:/", "") }

      val label = rdd.map { x =>
        val arr = x.split("/").takeRight(1).head.split("_")
        arr.slice(0,arr.length - 1).mkString("_")
      }

      // label.foreach(println)

      val labeledMap = label.zipWithIndex().collectAsMap().asInstanceOf[mutable.HashMap[String,Long]]

      val labeledMapRDD = sc.parallelize(labeledMap.toSeq)

      val r = labeledMapRDD.map(x => x._1 +"\t" + x._2).collect()

      // FileUtil.writeToFile("E:/github/lfw-labeledMap", r)
      // labeledMapRDD.saveAsTextFile("hdfs://61.147.114.85:9000/user/youchaojiang/lfw-labelMap")

      val pixels = rdd.map { f => extractPixels(f, 30 , 30) }

      // 为每张图片创建向量对象
      val vectors = pixels.map { x => Vectors.dense(x) }
      vectors.setName("image-vectors") // web 界面方便识别
      vectors.cache()

      // 正则化
      val scaler = new StandardScaler(withMean = true, withStd = false).fit(vectors) // 提取mean
      val scaledVectors = vectors.map { v => scaler.transform(v) } // 向量减去当前列的平均值

      val zipped = label.zip(vectors)

      val trainData = zipped.map { case (key, vector) => LabeledPoint(labeledMap(key), vector)}

      val model = NaiveBayes.train(trainData, lambda =  0.1 )

      // model.save(sc,"hdfs://61.147.114.85:9000/user/youchaojiang/lfw-model2")
      // model.save(sc,"E:/github/lfw-model")
      // TestData
      // 允许一次性操作整个文件，不同于之前的在一个文件或多个文件中只能逐行处理
      val testRDD = sc.wholeTextFiles("file:///E:/github/lfw-test/*").map { case (fileName, content) => fileName.replace("file:/", "") }

      val testLabel = testRDD.map { x =>
        val arr = x.split("/").takeRight(1).head.split("_")
        arr.slice(0,arr.length - 1).mkString("_")
      }

      val testPixels = testRDD.map { f => extractPixels(f, 30 , 30) }

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

      val labeledMapReverse = labeledMapRDD.map(x =>(x._2, x._1)).collectAsMap()

      val result = predictionAndLabels.filter(x => x._1 != x._2).map(x=> (x._1.toLong, x._2.toLong)).map(x => (labeledMapReverse(x._1),labeledMapReverse(x._2))).map(x => x._1 + "\t" + x._2).collect()

      FileUtil.writeToFile("E:/github/lfw-result",result)

      val metrics = new MulticlassMetrics(predictionAndLabels)
      println("加权F-指标：" + metrics.weightedFMeasure) // 加权F-指标：0.781142389463205

      // 比较结果是否在容忍的误差范围之内
      def approxEqual(array1: Array[Double], array2: Array[Double], tolerance: Double = 1e-6) = {

        val bools = array1.zip(array2).map{ case (v1, v2) => if(math.abs(math.abs(v1) - math.abs(v2)) > tolerance) false else true }

        bools.fold(true)(_ & _)

      }


    }

    // 测试集评估模型
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

      FileUtil.writeToFile("E:/github/lfw-result",result)

      val metrics = new MulticlassMetrics(predictionAndLabels)
      println("precision:" + metrics.precision)
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
