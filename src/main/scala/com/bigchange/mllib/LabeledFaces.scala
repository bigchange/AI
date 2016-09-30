package com.bigchange.mllib

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by C.J.YOU on 2016/9/23.
  * 降维模型
  * 数据降维： 使用人脸识别为实例
  */
object LabeledFaces {

  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("LabeledFaces").setMaster("local"))

  def main(args: Array[String]) {

   val path = "file:///E:/github/lfw/*/*"
    // 允许一次性操作整个文件，不同于之前的在一个文件或多个文件中只能逐行处理
    val rdd = sc.wholeTextFiles(path).map { case (fileName, content) => fileName.replace("file:/","") }

    rdd.foreach(println)

    val pixels = rdd.map { f => extractPixels(f, 50 , 50) }
    println(pixels.take(10).map(_.take(10).mkString("",",",",...")).mkString("\n"))

    // 为每张图片创建向量对象
    val vectors = pixels.map { x => Vectors.dense(x) }
    vectors.setName("image-vectors") // web 界面方便识别
    vectors.cache()

    // 正则化（均值减法处理）
    val scaler = new StandardScaler(withMean = true, withStd = false).fit(vectors) // 提取mean
    val scaledVectors = vectors.map { v => scaler.transform(v) } // 向量减去当前列的平均值

    // RowMatrix装换
    val matrix = new RowMatrix(scaledVectors)
    val k = 10 // 取主成成分向量值
    val pca = matrix.computePrincipalComponents(k) // PCA
    // val svd = matrix.computeSVD(k) // SVD

    // 可视化特征脸: 获取的k列主成成分理解为：包含原始数据主要变化特征的隐藏特征
    val rows = pca.numRows   // 50 * 50 : 每个向量的维度和图片的维度一样
    val cols = pca.numCols // 主成成分值
    println(rows, cols)
    import breeze.linalg.DenseMatrix
    val pcaBreeze = new DenseMatrix(rows, cols, pca.toArray)
    // 主成成分保存临时CSV
    // import   breeze.linalg.csvwrite
    // csvwrite(new File("/tmp/pca.csv"), pcaBreeze) // IPYTHON 提供加载矩阵并以图像的形式可视化主成成分

    // 将原始向量映射到主成成分表示的新的低维空间上
    val projected = matrix.multiply(pca) // 另一个机器模型的数据输入

    // SVD 和 PCA 的关系
    val svd = matrix.computeSVD(matrix.numCols().toInt, computeU = true)
    val U = svd.U //
    val S = svd.s // 奇异值向量
    val v = svd.V // 右奇异值向量 （注意观察和PCA计算出来的低维空间对比）

    // svd 一个良好性质是在它的返回值U中，特征向量是按照特征值的大小排列的。我们可以利用这个性质来对数据降维，只要使用前面的小部分特征向量，丢弃掉那些包含的数据没有方差的维度。 这个操作也被称为主成分分析（ Principal Component Analysis 简称PCA）降维

    // 比较结果是否在容忍的误差范围之内
    def approxEqual(array1: Array[Double], array2: Array[Double], tolerance: Double = 1e-6) = {

      val bools = array1.zip(array2).map{ case (v1, v2) => if(math.abs(math.abs(v1) - math.abs(v2)) > tolerance) false else true }

      bools.fold(true)(_ & _)

    }

    // U * S
    val breezeS = breeze.linalg.DenseVector(svd.s.toArray)
    val projectedSVD = svd.U.rows.map { v =>

      val breezeV = breeze.linalg.DenseVector(v.toArray)
      val multV = breezeV.:*(breezeS) // 向量对应元素相乘
      Vectors.dense(multV.data)

    }

    // 确定PCA映射后的低维空间值和SVD的映射值每行是否一样
    projected.rows.zip(projectedSVD).map{ case (v1, v2) => approxEqual(v1.toArray, v2.toArray) }.filter(b => b).count()

    //
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
