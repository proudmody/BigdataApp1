package com.triman.bigdata.util

/**
  * Created by hadoop on 2016/3/22.
  */
import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils

object SavaJdbc{
  def Save(df:DataFrame,table:String) {
//    val testBD = "jdbc:oracle:thin:@10.16.163.41:1521:ORCL"
//    val testUser = "zhyy"
//    val testPassword = "triman"
    val rkDB = "jdbc:oracle:thin:@10.15.58.19:1521:syrk1"
    val rkUser = "syrk"
    val rkPassword = "smc@23977"
    val connectionProperties = new Properties()
    connectionProperties.put("user",rkUser)
    connectionProperties.put("password",rkPassword)
    Class.forName("oracle.jdbc.driver.OracleDriver")
    JdbcUtils.saveTable(df,rkDB ,table,connectionProperties)

  }
  def Save(df:DataFrame,url:String,table:String,prop:Properties) {

    JdbcUtils.saveTable(df,url ,table,prop)
  }
}




