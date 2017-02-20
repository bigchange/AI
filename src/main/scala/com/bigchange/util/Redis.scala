package com.bigchange.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.util.{Failure, Success, Try}

/**
  * Created by Administrator on 2016/1/7.
  */
class Redis private (ip:String, port:String, auth:String, dataBase:String) {

  private var redis: Jedis = null

  private lazy val config = initConfig

  private var redisPool: JedisPool = getRedisPool

  def initConfig = {

    val config = new JedisPoolConfig()
    config.setMaxWaitMillis(10000)
    config.setMaxIdle(100)
    config.setMaxTotal(1024)
    config.setTestOnBorrow(false)

    config

  }

  def getRedisPool = new JedisPool(config, ip, port.toInt, 20000, auth, dataBase.toInt)

  def getRedis = {

    if(redisPool == null)
      redisPool = getRedisPool

    Try(redisPool.getResource) match {
      case Success(r) => redis = r
      case Failure(e) => {
        redisPool.close()
      }
    }

    redis

  }

  def closeRedisPool = if (redisPool !=null) redisPool.close()

}

object Redis {

  val redis: Redis = null

  def apply(ip:String, port:String, auth:String, dataBase:String): Redis = {

    if(redis == null)
      new Redis(ip, port, auth, dataBase)
    else redis

  }

}
