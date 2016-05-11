package com.bigchange.util

import java.util.Properties

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by C.J.YOU on 2016/3/22.
  */
object MysqlUtil {

  private val sparkConf = new SparkConf()
    .setAppName("LoadData_Analysis")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "2000")
    .setMaster("local")
  private val sc = new SparkContext(sparkConf)
  private val sqlContext = new SQLContext(sc)


  def getTableDataAndWriteData(inTableName:String,readConnection:String,outTableName:String,writeConnection:String): Unit ={
    val properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    // .write.mode("append").
    sqlContext.createDataFrame(sqlContext.read.jdbc(readConnection,inTableName,properties).map(row => Row(row(0),row(1),row(1).toString,row(3),row(1).toString,0)),schema =
      StructType(StructField("v_code", StringType, nullable = true) ::
        StructField("v_name", StringType, nullable = true) ::
        StructField("v_name_url", StringType, nullable = true) ::
        StructField("v_jian_pin", StringType, nullable = true) ::
        StructField("v_quan_pin", StringType, nullable = true) ::
        StructField("v_count", IntegerType, nullable = true)::Nil))
      .write.mode("append").jdbc(writeConnection,outTableName,properties)

  }

}
