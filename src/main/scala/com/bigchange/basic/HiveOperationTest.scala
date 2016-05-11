package com.bigchange.basic

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by CHAOJIANG on 2016/5/1 0001.
  */
object HiveOperationTest {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <inpath>")
      System.exit(1)
    }

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("HiveOperationTest")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // create table
    sqlContext.sql("CREATE TABLE IF NOT EXISTS weather (date STRING, city STRING, minTem Int, maxTem Int) row format delimited fields terminated by '\t'")
    sqlContext.sql(s"LOAD DATA INPATH '$inputFile' INTO TABLE weather")

    // Queries are expressed in HiveQL
    sqlContext.sql("select city, avg(minTem) from weather group by city").collect().foreach(println)

    // 使用 udf
    sqlContext.udf.register("class", (s: Int) => if (s <= 20) "lower" else "high")

    sqlContext.sql("select city, maxTem, class(maxTem) from weather").collect().foreach(println)

    sc.stop()
  }

}
