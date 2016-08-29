package com.triman.bigdata.syrk

/**
  * Created by hadoop on 2016/3/22.
  */
import java.text.SimpleDateFormat
import java.util.Date

import com.triman.bigdata.util.{HDFSUtil, Initial_Spark}
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer
object DataExtract{
  def main(args: Array[String]) {
    val d = new Date()
    val df1 = new SimpleDateFormat("yyyyMM")
    val date = df1.format(d)+"16"
    val (sc,sqlContext) = Initial_Spark.Initial_Spark("DataExtract",date)
    //数据抽取
    val jdbcDF1 = sqlContext.load("jdbc", Map(
     "url" -> "jdbc:oracle:thin:@:1521:",
      "dbtable" -> "t_rjbxx",
      "user" -> "",
      "password" -> "",
      "fetchSize" -> "100",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))
    jdbcDF1.write.parquet(date+"parquet/T_RJBXX")
    val jdbcDF2 = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:oracle:thin:@:1521:",
      "dbtable" -> "fwgl.t_fwglxx",
      "user" -> "",
      "password" -> "",
      "fetchSize" -> "100",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))
    jdbcDF2.write.parquet(date+"parquet/T_FWGLXX")
    val jdbcDF3 = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:oracle:thin:@:1521:",
      "dbtable" -> "fwgl.t_fwjbxx",
      "user" -> "",
      "password" -> "",
      "fetchSize" -> "",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))
    jdbcDF3.write.parquet(date+"parquet/T_FWJBXX")
    val jdbcDF4 = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:oracle:thin:@1521:",
      "dbtable" -> "fwgl.t_wsfhmlhry",
      "user" -> "",
      "password" -> "",
      "fetchSize" -> "100",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))
    jdbcDF4.write.parquet(date+"parquet/T_WSFHMLHRY")

    sc.stop()
  }
}




