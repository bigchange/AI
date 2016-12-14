package com.bigchange.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by C.J.YOU on 2016/1/8.
  */
object TimeUtil {

  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")

  def getDay: String = sdf.format(new Date).split("-").slice(0, 3).mkString("-")

  def getCurrentHour: Int = {

    val calendar = Calendar.getInstance
    calendar.setTime(new Date)
    calendar.get(Calendar.HOUR_OF_DAY)

  }

  def getTimeStamp: Long = System.currentTimeMillis()

  def formatTimeStampToHour(ts: String) = sdf.format(new Date(ts.toLong)).split("-").slice(0, 4).mkString("-")



}
