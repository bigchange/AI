package usecase

import java.sql.ResultSet

import com.bigchange.config.XMLConfig
import com.bigchange.spark.Spark
import com.bigchange.ssql.MysqlHandler
import com.bigchange.util.TimeUtil

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

/**
  * Created by C.J.YOU on 2016/12/14.
  * 统计2016-11月实时搜索和查看的数据（从数据库中读stock_visit_old 和 stock_search_old）
  */
object SHRealTimeSearchAndVisitCountByHour {


  def usingMysqlHandler(): Unit = {
    // 数据库连接
    val url = "jdbc:mysql://61.147.114.76:3306/stock?useUnicode=true&characterEncoding=utf8"
    val driver = "com.mysql.jdbc.Driver"

    val xmlconfig = XMLConfig.apply(xmlFilePath = "E:\\stock.xml")

    val table = "stock_search_check"
    val mysqlHandler = MysqlHandler.apply(url, xmlconfig)

    val startTs = "1477929600"
    val endTs  = "1480521599"

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

  def dateFormat(array: Array[String]) = {

    Array(array(0), TimeUtil.formatTimeStampToHour(array(1) + "000"), array(2))

  }

  def getDataFormCSV(args:Array[String]) = {

    val Array(son, von) = args

    val searchOn = sc.textFile(son).map(_.split(",")).map(dateFormat).map(x => ((x(0),x(1)),x(2))).sortByKey(ascending = false)

    val visitOn = sc.textFile(von).map(_.split(",")).map(dateFormat).map(x => ((x(0),x(1)),x(2))).sortByKey(ascending = false)

    val res = visitOn.leftOuterJoin(searchOn).map { x =>

      val key = x._1
      val value = x._2

      (key, (value._1, value._2.getOrElse("0-")))

    }.filter(_._2._2 == "0-").top(10).foreach(println)


  }

  val sc = Spark.apply("local", "Test", 60).sc

  def main(args: Array[String]) {

//    getDataFormCSV(args = Array(
//      "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\stock_search_check.csv",
//      "F:\\datatest\\telecom\\wokong\\上海数据统计文件\\离线和实时热度数据对比\\stock_visit_check.csv"
//    ))

   usingMysqlHandler()


  }

}
