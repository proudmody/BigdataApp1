package com.triman.bigdata.jf

/**
  * Created by hadoop on 2016/3/22.
  */
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}
import com.triman.bigdata.util.{HDFSUtil, Initial_Spark}

object ZDRY_JF_DATAEX{
  def main(args: Array[String]) {
    val d = new Date()
    val date4mat = new SimpleDateFormat("yyyyMMdd")
    val date = date4mat.format(d)
    val (sc,sqlContext) = Initial_Spark.Initial_Spark("ZDRY_JF_DATAEX",date)

    val jdbcDF1 = sqlContext.load("jdbc", Map(
     "url" -> "jdbc:oracle:thin:@",
      "dbtable" -> "t_zdry_jbxx_old",
      "user" -> " ",
      "password" -> "",
      "fetchSize" -> "100",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))
    jdbcDF1.write.parquet(date+"parquet/T_ZDRY_JBXX_OLD1")
    val jdbcDF2 = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:oracle:thin:@3",
      "dbtable" -> "wsba.hx_a_ajjbqk",
      "user" -> " ",
      "password" -> "",
      "fetchSize" -> "100",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))
    jdbcDF2.write.parquet(date+"parquet/HX_A_AJJBQK")
    val jdbcDF3 = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:oracle:thin:@",
      "dbtable" -> "vw_wsba_hx_r_xyrc",
      "user" -> " ",
      "password" -> "",
      "fetchSize" -> "100",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))
    jdbcDF3.write.parquet(date+"parquet/VW_WSBA_HX_R_XYRC")
    val jdbcDF4 = sqlContext.load("jdbc", Map(
        "url" -> "jdbc:oracle:thin:@",
        "dbtable" -> "",
        "user" -> "",
        "password" -> "",
        "fetchSize" -> "100",
        "driver" -> "oracle.jdbc.driver.OracleDriver"))
      jdbcDF4.write.parquet(date+"parquet/T_RB_SHBX")
    val jdbcDF5 = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:oracle:thin:@1",
      "dbtable" -> "",
      "user" -> "",
      "password" -> "smc@",
      "fetchSize" -> "100",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))
    jdbcDF5.write.parquet(date+"parquet/T_JDC")
    val jdbcDF6 = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:oracle:thin:@",
      "dbtable" -> "V_JSZ",
      "user" -> "",
      "password" -> "",
      "fetchSize" -> "100",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))
    jdbcDF6.write.parquet(date+"parquet/V_JSZ")

    val rkDB = "jdbc:oracle:thin:@:syrk1"
    val rkUser = ""
    val rkPassword = ""
    val connectionProperties = new Properties()
    connectionProperties.put("user",rkUser)
    connectionProperties.put("password",rkPassword)
    connectionProperties.put("driver","oracle.jdbc.driver.OracleDriver")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE,-14)
    val d_14 = calendar.getTime
    val d_now = new Date()
    val d4mat = new SimpleDateFormat("yyyyMMdd")
    val lowerbound =  d4mat.format(d_14)+"000000"
    val upbound = d4mat.format(d_now)+"000000"
    val df7 = sqlContext.read.jdbc(rkDB,"T_WB_TRACE",Array("LOGIN_AT BETWEEN '"+lowerbound+"' AND '"+ upbound +"'"),connectionProperties)
    df7.write.parquet(date+"parquet/T_WB_TRACE")

    sc.stop()
    return
  }
}




