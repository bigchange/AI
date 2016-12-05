package com.bigchange.util

import java.sql.Connection

import com.bigchange.config.XMLConfig
import com.jolbox.bonecp.{BoneCP, BoneCPConfig}

/**
  * Created by C.J.YOU on 2016/3/22.
  */
class MysqlTool private(val xmlHandle: XMLConfig, val isTestOrNot: Boolean = true) extends Serializable {

    lazy val config = createConfig

    lazy val connPool = new BoneCP(config)

    /** 初始化数据库连接池 */
    def createConfig: BoneCPConfig = {

      val initConfig = new BoneCPConfig

      if(isTestOrNot) {

        initConfig.setJdbcUrl(xmlHandle.getElem("mySql", "urltest"))
        initConfig.setUsername(xmlHandle.getElem("mySql", "usertest"))
        initConfig.setPassword(xmlHandle.getElem("mySql", "passwordtest"))

      } else {
        initConfig.setJdbcUrl(xmlHandle.getElem("mySql", "urlstock"))
        initConfig.setUsername(xmlHandle.getElem("mySql", "userstock"))
        initConfig.setPassword(xmlHandle.getElem("mySql", "passwordstock"))
      }

      initConfig.setMinConnectionsPerPartition(Integer.parseInt(xmlHandle.getElem("mySql", "minconn")))
      initConfig.setMaxConnectionsPerPartition(Integer.parseInt(xmlHandle.getElem("mySql", "maxconn")))
      initConfig.setPartitionCount(Integer.parseInt(xmlHandle.getElem("mySql", "partition")))
      initConfig.setConnectionTimeoutInMs(Integer.parseInt(xmlHandle.getElem("mySql", "timeout")))
      initConfig.setConnectionTestStatement("select 1")
      initConfig.setIdleConnectionTestPeriodInMinutes(Integer.parseInt(xmlHandle.getElem("mySql", "connecttest")))

      initConfig

    }

    def setConfig(mix: Int, max: Int, testPeriod: Long) = {
      config.setPartitionCount(1)
      config.setMinConnectionsPerPartition(mix)
      config.setMaxConnectionsPerPartition(max)
      config.setIdleConnectionTestPeriodInMinutes(3)
      config.setIdleMaxAgeInMinutes(3)
    }

    /**
      * 获取连接
      * @author wukun
      */
    def getConnect: Option[Connection] = {

      var connect: Option[Connection] = null

      try {
        connect = Some(connPool.getConnection)
      } catch {

        case e: Exception => {

          if(connect != null) {
            connect.get.close
          }
          connect = None
        }
      }

      connect
    }

    def close = connPool.shutdown

  }

  /**
    * Created by wukun on 2016/5/18
    * MysqlPool伴生对象
    */
  object MysqlTool extends Serializable {

    def apply(xmlHandle: XMLConfig, isTestOrNot: Boolean = true) = new MysqlTool(xmlHandle, isTestOrNot)


  }
