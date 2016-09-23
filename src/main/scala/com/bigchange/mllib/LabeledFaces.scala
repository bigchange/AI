package com.bigchange.mllib

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by C.J.YOU on 2016/9/23.
  */
object LabeledFaces {

  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("LabeledFaces").setMaster("local"))

  def main(args: Array[String]) {

    val path = "/Path/lfw/*"
    // 允许一次性操作整个文件，不同于之前的在一个文件或多个文件中只能逐行处理
    val rdd = sc.wholeTextFiles(path).map { case (fileName, content) => fileName.replace("file:","") }



  }
  // 彩色图片： 三维像素数组或矩阵，（x, y, RGB三元色的值），灰度图片：每个像素只需要一个值表示。
  // pic load:
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






}
