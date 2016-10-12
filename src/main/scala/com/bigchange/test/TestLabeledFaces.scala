package com.bigchange.test

import com.bigchange.mllib.LabeledFaces
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix

/**
  * Created by CHAOJIANG on 2016/9/25 0025
  * 降维模型如何使用？？
  */
object TestLabeledFaces {

  def main(args: Array[String]): Unit = {

    val path = "file:///J:\\github\\dataSet\\lfw-a\\lfw\\*"
    // 允许一次性操作整个文件，不同于之前的在一个文件或多个文件中只能逐行处理
    val rdd = LabeledFaces.sc.wholeTextFiles(path).map { case (fileName, content) => fileName.replace("file:/","") }

    rdd.foreach(println)

    val pixels = rdd.map { f => LabeledFaces.extractPixels(f, 50 , 50) }

    // 为每张图片创建向量对象
    val vectors = pixels.map { x => Vectors.dense(x) }
    vectors.setName("image-vectors") // web 界面方便识别
    vectors.cache()

    // 正则化
    val scaler = new StandardScaler(withMean = true, withStd = false).fit(vectors) // 提取mean
    val scaledVectors = vectors.map { v => scaler.transform(v) } // 向量减去当前列的平均值

    // RowMatrix装换
    val matrix = new RowMatrix(scaledVectors)
    val k = 10 // 取主成成分向量值

    // 在数据集上估计SVD的k值
    // val sValues = (1 to 5).map { i => matrix.computeSVD(i, computeU =  false).s }.foreach(println) // 每次运算的结果相同，并递减返回

    // 估计聚类的k值，以一个较大的k变化范围绘制一个奇异值图，看看每增加一个奇异值的变化总量是否保持基本不变
    val svd300 = matrix.computeSVD(300, computeU =  false)


    /*val sMatrix = new DenseMatrix(1, 300, svd300.s.toArray)
    import   breeze.linalg.csvwrite
    csvwrite(new File("/tmp/csv"), sMatrix)
    // 通过IPython 画图可以清楚看到
    s = np.loadtext("/tmp/csv", delimiter = "")
    println(s.shape)
    plot(s)*/


  }
}
