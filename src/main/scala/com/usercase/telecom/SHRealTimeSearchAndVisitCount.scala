package com.usercase.telecom

import java.io.File
import java.sql.ResultSet

import com.bigchange.config.XMLConfig
import com.bigchange.spark.Spark
import com.bigchange.ssql.MysqlHandler
import com.bigchange.util.{FileUtil, TimeUtil}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

/**
  * Created by C.J.YOU on 2016/12/14.
  * 统计2016-11月实时搜索和查看的数据（从数据库中读stock_visit_old 和 stock_search_old）
  * 对比离线 和 实时 热度数据
  */
object SHRealTimeSearchAndVisitCount {


  def usingMysqlHandler(): Unit = {
    // 数据库连接
    val url = "jdbc:mysql://61.147.114.76:3306/stock?useUnicode=true&characterEncoding=utf8"
    val driver = "com.mysql.jdbc.Driver"

    val xmlconfig = XMLConfig.apply(xmlFilePath = "E:\\stock.xml")

    val table = "stock_visit_old"
    val mysqlHandler = MysqlHandler.apply(url, xmlconfig)

    val rs = mysqlHandler.executeQuery(String.format("select stock_code,timeTamp,count from " + table + " where timeTamp >=%s and timeTamp<=%s",startTs, endTs))

    val result = rs match {
      case Success(r) => r
      case Failure(ex) => println(ex.getMessage)
     }

    val RS = result.asInstanceOf[ResultSet]

    val seqSearch = new ListBuffer[String]

    while(RS.next()) {

      val sc = RS.getString(1)
      val ts = RS.getInt(2)
      val count = RS.getInt(3)
      val rs = sc + "\t" + ts + "\t" + count

      seqSearch.+=(rs)

    }

    val data = sc.parallelize(seqSearch)

    println(data.count())


  }

  def dateFormat(array: Array[String], length:Int) = {

    Array(array(0), TimeUtil.formatTimeStamp(array(1) + "000", 0, 4), array(2))

  }

  def filter(args:Array[String]) = {

    if(args(1) >= startTs && args(1) <= endTs) true else  false

  }

  // 按小时分类的数据(STOCK, TS, COUNT)
  def dataFormat(rdd:RDD[String], length:Int) = {

    rdd.map(_.split(",")).filter(x => x.length == 3 &&  !x(0).contains("stock_code"))
      .filter(filter)
      .map(dateFormat(_, length)).map(x => ((x(0),x(1)),x(2).toInt))

      .reduceByKey(_ + _).sortByKey(ascending = false)
  }

  def dataMap(rdd:RDD[String]) = {

    rdd.map(_.split(",")).filter(x => x.length == 3 &&  !x(0).contains("stockCode")).map(x => ((x(0),x(1)), x(2).toInt))

  }

  // stock, ts, count 格式(将 ts -> hour )  实时 和 离线
  def getDataFormCSVByHour(args:Array[String]) = {

    val Array(son, von) = args

    val filePath = son.split("\\\\")
    val savePath = filePath.slice(0,filePath.length - 4).mkString("\\")


    // 按月保存每天，每小时的统计量
    if(new File(son).exists()) {
      val searchOn = dataFormat(sc.textFile(son), 4).keyBy(_._1._2.split("-").slice(0, 3).mkString("-"))
        .groupByKey().foreach { x =>
        FileUtil.writeToCSV(savePath + "\\11月\\11月离线按小时在线search数据\\" + x._1 + ".csv", Array("stockCode", "TimeStamp", "Count"), x._2.toArray)
      }
    }
    if(new File(von).exists()) {
      val visitOn = dataFormat(sc.textFile(von), 4).keyBy(_._1._2.split("-").slice(0, 3).mkString("-"))
        .groupByKey().foreach { x =>
        FileUtil.writeToCSV(savePath + "\\11月\\11月离线按小时在线visit数据\\" + x._1 + ".csv", Array("stockCode", "TimeStamp", "Count"), x._2.toArray)
      }
    }
  }
  //  按小时进行三月对比 （stock, ts, count） - 实时、离线
  def getThreeMonthPerHourCmp(args:Array[String], typeS: String)  = {

    val Array(nineP, tenP, elevP) = args
    val savePath = "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比"
    val nine = dataMap(sc.textFile(nineP)).map(x => ((x._1._1, x._1._2.split("-").slice(2, 4).mkString("->")), x._2))
    val ten = dataMap(sc.textFile(tenP)).map(x => ((x._1._1, x._1._2.split("-").slice(2, 4).mkString("->")), x._2))
    val elev = dataMap(sc.textFile(elevP)).map(x => ((x._1._1, x._1._2.split("-").slice(2, 4).mkString("->")), x._2))

    val x = elev.rightOuterJoin(ten).map { x =>
      val key = x._1
      val value = x._2
      (key, (value._1.getOrElse(0), value._2))
    }.leftOuterJoin(nine).map { x =>
      val key = x._1
      val value = x._2
      (key, (value._1._1, value._1._2, value._2.getOrElse(0)))
    }.sortBy(x => x._1._1 + "\t" + x._1._2).collect()

    FileUtil.writeToCSVCmp(savePath + "\\" + typeS + "CMP.csv", Array("stockCode", "TimeStamp", "11Month", "10Month", "9Month"), x)

  }

  //  按day进行三月对比 （stock, ts, count） - 实时、离线
  def getThreeMonthPerDayCmp(args:Array[String], typeS: String)  = {

    val Array(nineP, tenP, elevP) = args

    val savePath = "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比"

    val nine = dataMap(sc.textFile(nineP)).map(x => ((x._1._1, x._1._2.split("-").slice(2, 3).mkString("->")), x._2))
    val ten = dataMap(sc.textFile(tenP)).map(x => ((x._1._1, x._1._2.split("-").slice(2, 3).mkString("->")), x._2))
    val elev = dataMap(sc.textFile(elevP)).map(x => ((x._1._1, x._1._2.split("-").slice(2, 3).mkString("->")), x._2))

    val x = elev.rightOuterJoin(ten).map { x =>
      val key = x._1
      val value = x._2
      (key, (value._1.getOrElse(0), value._2))
    }.leftOuterJoin(nine).map { x =>
      val key = x._1
      val value = x._2
      (key, (value._1._1, value._1._2, value._2.getOrElse(0)))
    }.sortBy(x => x._1._1 + "\t" + x._1._2).collect()

    FileUtil.writeToCSVCmp(savePath + "\\" + typeS + "CMP.csv", Array("stockCode", "TimeStamp", "11Month", "10Month", "9Month"), x)

  }


  // stock, ts, count 格式 （实时和离线）
  def getDataFormCSVByDay(args:Array[String]) = {

    val Array(son, von) = args

    val filePath = son.split("\\\\")
    val savePath = filePath.slice(0, filePath.length - 4).mkString("\\")

     // 按月保存每天 的统计量
    if(new File(von).exists()) {
      val visitOn = dataFormat(sc.textFile(von), 3).keyBy(_._1._2)
        .groupByKey().foreach { x =>
        FileUtil.writeToCSV(savePath + "\\11月\\11月离线按天在线visit\\" + x._1 + ".csv", Array("stockCode", "TimeStamp", "VisitCount"), x._2.toArray)
      }
    }
    if(new File(son).exists()) {
      val searchOn = dataFormat(sc.textFile(son), 3).keyBy(_._1._2)
        .groupByKey().foreach { x =>
        FileUtil.writeToCSV(savePath + "\\11月\\11月离线按天在线search\\" + x._1 + ".csv", Array("stockCode", "TimeStamp", "SearchCount"), x._2.toArray)
      }
    }
  }

  // 三个月，按照每天每只股票 数量对比放在三列中，方便查看(在线)
  // stock,day1,day2,....（实时）
  def dataFormat(rdd:RDD[String] ) = {

    rdd.flatMap { x =>

      val listBuffer = new ListBuffer[((String,String), Int)]

      val xs = x.split(",")
      val stock = xs(0)
      for(index <- 1 until xs.length) {
        val day = if(index < 10) "day_0" + index else "day_" + index
        listBuffer.+=(((stock ,day), xs(index).toInt))
      }
      listBuffer
    }

  }
  // 实时
  def getThreeMonthOnlinePerDayCmp(args:Array[String], typeS: String)  = {

    val Array(nineP, tenP, elevP) = args

    val filePath = nineP.split("\\\\")
    val savePath = filePath.slice(0,filePath.length -1).mkString("\\")

    val nine = dataFormat(sc.textFile(nineP))
    val ten = dataFormat(sc.textFile(tenP))
    val elev = dataFormat(sc.textFile(elevP))

    val x = elev.rightOuterJoin(ten).map { x =>
      val key = x._1
      val value = x._2
      (key, (value._1.getOrElse(0), value._2))
    }.leftOuterJoin(nine).map { x =>
      val key = x._1
      val value = x._2
      (key, (value._1._1, value._1._2, value._2.getOrElse(0)))
    }.sortBy(x => x._1._1 + "\t" + x._1._2).collect()

      FileUtil.writeToCSVCmp(savePath + "\\" + typeS + "CMP.csv", Array("stockCode", "TimeStamp", "11", "10", "9"), x)


  }


  val sc = Spark.apply("local", "Test", 60).sc

  // 11
    val startTs = "1477929600"
    val endTs  = "1480521599"

  // 10
//  val startTs = "1475251200"
//  val endTs  = "1477929599"

  // 9
//   val startTs = "1472659200"
//   val endTs  = "1477843199"

  def main(args: Array[String]) {

    // 按天 实时 三月对比
//    getThreeMonthPerDayCmp(args = Array(
//      "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\stock_search_month_9.csv",
//      "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\stock_search_month_10.csv",
//      "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\stock_search_month_11.csv"
//    ), "search")


//     for(d <- 1 to 31) {
//          val day = if (d < 10) "0" + d else d
//          // 按小时 分开
//              getDataFormCSVByHour(args = Array(
//                "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\离线原始数据\\stock_search\\11月\\2016-11-" + day + ".csv",
//                "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\离线原始数据\\stock_visit\\11月\\2016-11-" + day + ".csv"
//              ))
//        }


//    for(d <- 1 to 31) {
//      val day = if(d < 10) "0" + d  else d
//      // 按天 分开
//      getDataFormCSVByDay(args = Array(
//        "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\离线原始数据\\stock_search\\11月\\2016-11-" + day + ".csv",
//        "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\离线原始数据\\stock_visit\\11月\\2016-11-" + day + ".csv"
//      ))
//
//    }


  //  按小时三月对比
    getThreeMonthPerHourCmp(args = Array(
      "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\9月\\9月离线按小时在线search数据\\*",
      "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\10月\\10月离线按小时在线search数据\\*",
      "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\11月\\11月离线按小时在线search数据\\*"
    ), "search")



    // 按天 离线 三月对比
//    getThreeMonthPerDayCmp(args = Array(
//          "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\9月\\9月离线按天在线search\\*",
//          "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\10月\\10月离线按天在线search\\*",
//          "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\11月\\11月离线按天在线search\\*"
//        ), "search")


  }

}
