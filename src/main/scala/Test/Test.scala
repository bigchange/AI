package test

import org.apache.spark.{SparkContext, SparkConf}
import sun.misc.BASE64Encoder

import scala.collection.mutable

import scala.telecom.DataAnalysis
import scala.util.StringUtil

/**
  * Created by C.J.YOU on 2016/3/14.
  */

object Test {
  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("USER")

  val sc = new SparkContext(sparkConf)
  var string = ""

  private  val hashSet  = mutable.HashSet(",cms,",",tmeng,")

  def main(args: Array[String]) {

    // sc.wholeTextFiles("hdfs://server:9000/data/kafka").foreach(println)

    val str = "absolut.com"
    /*val urlDivide = str.split("\\.")
    breakable {
      for(item <- urlDivide ){
        if(globalString.contains(","+item+",")){
          println(item)
          break()
        }
      }
    }*/

     /*val id = ""
     val value = null
     val arr  =Array(id,value)
    val res = toJson2(arr)
    println(res)*/

    /*sc.textFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\rich_info.info")
      .map(line =>{
        var index = 0
        val arr  = line.split("\t")
        if(!arr.contains("null")) index = 1
        if(arr(1) == "null"){
          index = 3
        }
        if(arr(2) =="null") index = 2
        if(arr(1) =="null" && arr(2) =="null") index = 4
        (index,line)
      }).sortBy(_._1,ascending = true)
      .foreach(println)*/


    /*val dfsWordCount = sc.textFile("")
      .flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.size > 0)
      .map(w => (w, 1))
      .countByKey()
      .values
      .sum*/

   /* val c = sc.textFile("F:\\datatest\\telecom\\16").count()
    val r = sc.textFile("F:\\datatest\\telecom\\16").map(_.split("\t")).filter(_.size == 6).count()
    println(c - r)*/



   /* val res = sc.textFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海电信\\stock_20160330_key").map(_.split(",")).collect()(0).filter(_.length >=3).distinct.foreach(x => string = string +","+x)
    FileUtil.writeToFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海电信\\stock_key",Array(string))*/

    /*sc.parallelize((globalString + globalStringTwo).split(",")).filter(!_.isEmpty).filter(_.length >=3).foreach(x => string = string +","+x)
    FileUtil.writeToFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海电信\\final_url_20161443",Array(string))*/

   /* val res = StringUtil.index_op(globalStringOptimSplit + globalStringOptim,",weibo,",0)
    println(res)*/




  }

  private def toJson2(arr: Array[String]): String = {
    val json = "{\"id\":\"%s\", \"value\":\"%s\"}"
    json.format(arr(0), arr(1))
  }

  def runLocalWordCount(fileContents: List[String]): Int = {
    fileContents.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .groupBy(w => w)
      .mapValues(_.size)
      .values
      .sum
  }


}
