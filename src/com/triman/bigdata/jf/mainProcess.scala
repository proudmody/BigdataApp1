package com.triman.bigdata.jf

/**
  * 重点人员盗车内物
  */
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import com.triman.bigdata.util.{Initial_Spark, SavaJdbc}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}

object mainProcess {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val d = new Date()
    val df = new SimpleDateFormat("yyyyMMdd")
    val tjsj = df.format(d)
    val (sc,sqlContext) = Initial_Spark.Initial_Spark("mainProcess_JF",tjsj)
    //加载数据表
    val base = "hdfs://linux90:9000/user/hadoop/"+tjsj+"parquet/"
    val T_ZDRY_JBXX = sqlContext.read.parquet(base+"T_ZDRY_JBXX_OLD1").select("ZJHM","CSRQ","XB","JG")
    val HX_A_AJJBQK = sqlContext.read.parquet(base+"HX_A_AJJBQK").selectExpr("GUID","AJBH AS AJBH1","ZABMC","AJZTDM")
    val VW_WSBA_HX_R_XYRC = sqlContext.read.parquet(base+"VW_WSBA_HX_R_XYRC").selectExpr("SFZHM","AJBH AS AJBH2")
    val T_WB_TRACE = sqlContext.read.parquet(base+"T_WB_TRACE").select("LOGIN_AT","ID_CODE","AREA_CODE")
    val T_RB_SHBX = sqlContext.read.parquet(base+"T_RB_SHBX").selectExpr("ZJHM AS SBZJHM").distinct()
    val V_JSZ =  sqlContext.read.parquet(base+"V_JSZ").selectExpr("ZJHM AS JSZZJHM").distinct()
    val T_JDC =  sqlContext.read.parquet(base+"T_JDC").selectExpr("ZJHM AS JDCZJHM").distinct()
    //可以优化
    val df1 = P_PQLB_1.compute(sqlContext,T_ZDRY_JBXX,HX_A_AJJBQK,VW_WSBA_HX_R_XYRC,T_WB_TRACE,T_RB_SHBX,V_JSZ,T_JDC)
    val df2 = P_LQ_2.compute(sqlContext,T_ZDRY_JBXX,HX_A_AJJBQK,VW_WSBA_HX_R_XYRC,T_WB_TRACE,T_RB_SHBX,V_JSZ,T_JDC)
    val df3 = P_JMZP_3.compute(sqlContext,T_ZDRY_JBXX,HX_A_AJJBQK,VW_WSBA_HX_R_XYRC,T_WB_TRACE,T_RB_SHBX,V_JSZ,T_JDC)
    val df4 = P_RSDQ_4.compute(sqlContext,T_ZDRY_JBXX,HX_A_AJJBQK,VW_WSBA_HX_R_XYRC,T_WB_TRACE,T_RB_SHBX,V_JSZ,T_JDC)
    val df5 = P_DCNW_5.compute(sqlContext,T_ZDRY_JBXX,HX_A_AJJBQK,VW_WSBA_HX_R_XYRC,T_WB_TRACE,T_RB_SHBX,V_JSZ,T_JDC)
    val tcnt = T_ZDRY_JBXX.count()
//    val df1cnt = df1.count
//    val df2cnt = df2.count
//    val df3cnt = df3.count
//    val df4cnt = df4.count
//    val df5cnt = df5.count
//    println(df1cnt+" "+df2cnt+" "+df3cnt+" "+df4cnt+" "+df5cnt+" "+tcnt)
//    val df12 = df1.join(df2,df1("ZJHM1")===df2("ZJHM2"),"left")
//    val df13 = df1.join(df3,df1("ZJHM1")===df3("ZJHM3"),"left")
//    val df14 = df1.join(df4,df1("ZJHM1")===df4("ZJHM4"),"left")
//    val df15 = df1.join(df5,df1("ZJHM1")===df5("ZJHM5"),"left")
//        val df1cnt = df12.count
//        val df2cnt = df13.count
//        val df3cnt = df14.count
//        val df4cnt = df15.count
//        println(df1cnt+" "+df2cnt+" "+df3cnt+" "+df4cnt+" "+tcnt)
    val df12345 = df1.join(df2,df1("ZJHM1")===df2("ZJHM2"),"left").join(df3,df1("ZJHM1")===df3("ZJHM3"),"left")
      .join(df4,df1("ZJHM1")===df4("ZJHM4"),"left").join(df5,df1("ZJHM1")===df5("ZJHM5"),"left")
      .selectExpr("ZJHM1", "STOTAL1", "SQK1", "STOTAL2", "SQK2", "STOTAL3", "SQK3",  "STOTAL4", "SQK4", "STOTAL5","SQK5")

    val User = "priapweb"
    val Password = "priapweb"
    val connectionProperties = new Properties()
    connectionProperties.put("user",User)
    connectionProperties.put("password",Password)
    SavaJdbc.Save(df12345,"jdbc:oracle:thin:@10.15.61.36:1521:jzpt","T_JF_TMP",connectionProperties)
    //println(df12345.count())
  //  df12345.write.parquet("JFres")

//    val cnt = df12345.count()
//    df12345.head(100).foreach(println)
//    println(cnt)
    sc.stop()
    return
  }
}


//        var  con:Connection= null
//        try {
//          Class.forName("oracle.jdbc.driver.OracleDriver")
//          val url = "jdbc:oracle:thin:@10.15.61.36:1521:jzpt"
//          val user = "priapweb"
//          val password = "priapweb"
//          con = DriverManager.getConnection(url, user, password)
//        }
//        catch {
//          case e:Exception => e.printStackTrace();
//        }

