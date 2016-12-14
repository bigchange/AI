package com.bigchange.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by C.J.YOU on 2016/12/14.
  * spark 入口封装
  */
class Spark(master: String, appName:String, batchDuration: Int) {

  // conf的key=value具体配置放在shell脚本中传入
  // --conf "key=value"
  // --conf "spark.some.config.option=some-value"
  // --conf "spark.sql.warehouse.dir=spark-warehouse"

  val sparkConf = new SparkConf().setMaster(master).setAppName(appName)

  val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))

  val sc = ssc.sparkContext

  val sessionBuilder = SparkSession.builder().appName(appName).master(master)


}
// 伴生对象
object Spark {

  private  var sparkHandler: Spark = null

  def apply(master: String, appName: String, batchDuration: Int): Spark = {
    if(sparkHandler == null) sparkHandler = new Spark(master, appName, batchDuration)
    sparkHandler
  }

  def getInstance = sparkHandler

}

