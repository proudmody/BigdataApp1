package com.triman.bigdata.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by kjk on 2016/8/24.
  */
object DateUtil{

  def main(args: Array[String])  {
    println(getWcn())
    println(getMonthB4())
  }
  //返回未成年生日边界字符串
  def getWcn():String = {
    val d = new Date()
    d.setYear(d.getYear - 18)
    val df1 = new SimpleDateFormat("yyyyMMdd")
    val wcn = df1.format(d)
    wcn
  }
  //返回本月和之前3个月字符串
  def getMonthB4()={
    val df2 = new SimpleDateFormat("yyyyMM")
    val calendar = Calendar.getInstance()
    val tjsj = df2.format(calendar.getTime)
    calendar.add(Calendar.MONTH,-1)
    val tjsj1 = df2.format(calendar.getTime)
    calendar.add(Calendar.MONTH,-1)
    val tjsj2 = df2.format(calendar.getTime)
    calendar.add(Calendar.MONTH,-1)
    val tjsj3 = df2.format(calendar.getTime)
    calendar.add(Calendar.MONTH,-1)
    (tjsj,tjsj1,tjsj2,tjsj3)
  }
  //判断是否过了2或3月
  def getMonthMod()={
    val df2 = new SimpleDateFormat("yyyyMM")
    val tjsj = df2.format((new Date))
    val month = tjsj.substring(4, 6)
    val month_mod2 = (month.toInt % 2)
    val month_mod3 = (month.toInt % 3)
    (month_mod2,month_mod3)
  }
}
