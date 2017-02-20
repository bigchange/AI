package com.bigchange.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by C.J.YOU on 2016/12/14.
  * spark 入口封装
  */
class Spark(master: String, appName:String, batchDuration: Int, checkpoint_dir:String = ".") {

  // conf的key=value具体配置放在shell脚本中传入
  // --conf "key=value"
  // --conf "spark.some.config.option=some-value"
  // --conf "spark.sql.warehouse.dir=spark-warehouse"

  val sparkConf = new SparkConf().setMaster(master).setAppName(appName)

  // checkout point
  def createStreamingContext: () => StreamingContext = {
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    // NOTE: You have to create dstreams inside the method
    // See http://stackoverflow.com/q/35090180/1305344

    // data process procedure
    // .....

    ssc.checkpoint(checkpoint_dir)

    () => ssc

  }

  // 2.0.2 使用SparkSession
  @deprecated
  val ssc = StreamingContext.getOrCreate(checkpoint_dir, creatingFunc = createStreamingContext)

  // val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))

  @deprecated
  val sc = ssc.sparkContext

  // val sessionBuilder = SparkSession.builder().appName(appName).master(master)  // @spark 2.0+


}
// 伴生对象
object Spark {

  private var sparkHandler: Spark = null

  def apply(master: String, appName: String, batchDuration: Int): Spark = {

    if(sparkHandler == null) sparkHandler = new Spark(master, appName, batchDuration)

    sparkHandler

  }

  def getInstance = sparkHandler

}

