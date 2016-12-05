package com.bigchange.util

import java.io.PrintWriter
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by C.J.YOU on 2016/1/14.
  * HDFS操作的工具类
  */
object HDFSFileUtil {

  val conf = new Configuration()
  // 配置文件的设置
  conf.set("fs.defaultFS", "hdfs://ns1"); // nameservices的路径
  conf.set("dfs.nameservices", "ns1");  //
  conf.set("dfs.ha.namenodes.ns1", "nn1,nn2"); //namenode的路径
  conf.set("dfs.namenode.rpc-address.ns1.nn1", "server3:9000"); // namenode 通信地址
  conf.set("dfs.namenode.rpc-address.ns1.nn2", "server4:9000"); // namenode 通信地址
  // 设置namenode自动切换的实现类
  conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

  /** 创建目录 */
  def mkDir(name: String): Unit = {
    val fs = FileSystem.get(new URI("hdfs://ns1"), conf, "root")
    fs.mkdirs(new Path(name))
    fs.close()
  }

  /** 创建文件 */
  def createFile(name:String): Unit ={
    val fs = FileSystem.get(new URI("hdfs://ns1"), conf, "root")
    fs.createNewFile(new Path(name))
    fs.close()
  }

  /** 写入文件 */
  def writeToFile(name:String,array: Array[String]): Unit ={
    val fs = FileSystem.get(new URI("hdfs://ns1"), conf, "root")
    val writer = new PrintWriter(fs.append(new Path(name)))
    for (arr <- array){
      writer.append(arr + "\n")
    }
    writer.flush()
    writer.close()
    fs.close()
  }

  /** 判断文件是否存在 */
  def isExist(name:String): Boolean = {
    val fs = FileSystem.get(new URI("hdfs://ns1"), conf, "root")
    fs.exists(new Path(name))
  }



}
