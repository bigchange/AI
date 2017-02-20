package com.usercase.telecom

import com.bigchange.config.Dom4jParser
import com.bigchange.kafka.KafkaConsumer
import com.bigchange.spark.Spark
import com.bigchange.util.{FileUtil}

/**
  * Created by C.J.YOU on 2016/12/19.
  * 在线股票热度数据 保存
  */
object SHRealTimeStockHeatData {

  def main(args: Array[String]) {

    if(args.length < 1) {

      sys.error("args [xmlFile]")
      sys.exit(-1)

    }

    val parser = Dom4jParser.apply(xmlFilePath = args(0))

    val savePath = parser.getParameterByTagName("data.savePath")

    val ssc = Spark.apply(parser.getParameterByTagName("spark.master"), parser.getParameterByTagName("spark.appName"), parser.getParameterByTagName("spark.batchDuration").toInt).ssc

    val kafkaData = KafkaConsumer.apply(parser).getStreaming(ssc)

    kafkaData.map(_._2).foreachRDD { rdd =>

      val data = rdd.collect()
      FileUtil.saveData(savePath, data)

    }

    ssc.start()
    ssc.awaitTermination()

  }

}
