package com.bigchange.basic

/**
  * Created by CHAOJIANG on 2016/5/1 0001.
  */
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
 * 统计指定目录下的 words 数目，注意会对新的 text files 进行统计.
 * 文件放到目录中需要是采用 mv, rename 等原子方式进行的
 */
object MonitorHDFSDirFiles {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <directory>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("MonitorHDFSDirFiles")

    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).foreachRDD(rdd =>{
      val arr = rdd.collect()
      arr.foreach(println)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
