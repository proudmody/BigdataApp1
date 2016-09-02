package com.triman.bigdata.jf

/**
  * 一个测试用的
  */

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import com.triman.bigdata.util.{Initial_Spark, SavaJdbc}


object TableSchema{

  def main(args: Array[String]) {
    //初始化
    val d = new Date()
    val date4mat = new SimpleDateFormat("yyyyMMdd")
    val date = date4mat.format(d)
    val (sc,sqlContext) = Initial_Spark.Initial_Spark("TableSchema",date)
    //读取parquet文件
    val base = "hdfs://linux90:9000/user/hadoop/20160822parquet/"
    val T_ZDRY_JBXX = sqlContext.read.parquet(base+"T_ZDRY_JBXX_OLD1")
    val JFres = sqlContext.read.parquet("JFres")
    val out = JFres.selectExpr("ZJHM1", "STOTAL1", "SQK1", "STOTAL2", "SQK2",
  "STOTAL3", "SQK3",  "STOTAL4", "SQK4", "STOTAL5","SQK5")
    // 创建链接配置
    val User = "priapweb"
    val Password = "priapweb"
    val connectionProperties = new Properties()
    connectionProperties.put("user",User)
    connectionProperties.put("password",Password)
    //保存Dataframe到外部数据库
    SavaJdbc.Save(out,"t","T_JF_TMP",connectionProperties)
    sc.stop()
  }
}




