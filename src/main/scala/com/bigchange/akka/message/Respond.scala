package com.bigchange.akka.message

/**
  * Created by C.J.YOU on 2016/8/31.
  * 何时知道这个respond中数据已经存在了呢？
  */
class Respond {

  private var result: Result = null

  def getResult = result

  def setResult(resultValue: Result) = {
    result = resultValue
  }
}

object Respond {

  private var respond: Respond = null

  def apply: Respond = {

    if(respond == null){
      respond = new Respond()
    }
    respond

  }

  def getInstance = apply

}
