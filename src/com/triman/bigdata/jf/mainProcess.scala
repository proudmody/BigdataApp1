package com.triman.bigdata.jf

/**
  * 主程序
  */
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import com.triman.bigdata.util.{Initial_Spark, SavaJdbc}
import org.apache.log4j.{Level, Logger}

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
    //计算每个存储过程的结果
    //可以优化
    val df1 = P_PQLB_1.compute(sqlContext,T_ZDRY_JBXX,HX_A_AJJBQK,VW_WSBA_HX_R_XYRC,T_WB_TRACE,T_RB_SHBX,V_JSZ,T_JDC)
    val df2 = P_LQ_2.compute(sqlContext,T_ZDRY_JBXX,HX_A_AJJBQK,VW_WSBA_HX_R_XYRC,T_WB_TRACE,T_RB_SHBX,V_JSZ,T_JDC)
    val df3 = P_JMZP_3.compute(sqlContext,T_ZDRY_JBXX,HX_A_AJJBQK,VW_WSBA_HX_R_XYRC,T_WB_TRACE,T_RB_SHBX,V_JSZ,T_JDC)
    val df4 = P_RSDQ_4.compute(sqlContext,T_ZDRY_JBXX,HX_A_AJJBQK,VW_WSBA_HX_R_XYRC,T_WB_TRACE,T_RB_SHBX,V_JSZ,T_JDC)
    val df5 = P_DCNW_5.compute(sqlContext,T_ZDRY_JBXX,HX_A_AJJBQK,VW_WSBA_HX_R_XYRC,T_WB_TRACE,T_RB_SHBX,V_JSZ,T_JDC)
    //join 合并结果 按顺序select 需要保存到外部数据库的字段
    val df12345 = df1.join(df2,df1("ZJHM1")===df2("ZJHM2"),"left").join(df3,df1("ZJHM1")===df3("ZJHM3"),"left")
      .join(df4,df1("ZJHM1")===df4("ZJHM4"),"left").join(df5,df1("ZJHM1")===df5("ZJHM5"),"left")
      .selectExpr("ZJHM1", "STOTAL1", "SQK1", "STOTAL2", "SQK2", "STOTAL3", "SQK3",  "STOTAL4", "SQK4", "STOTAL5","SQK5")
    //创建配置
    val User = "priapweb"
    val Password = "priapweb"
    val connectionProperties = new Properties()
    connectionProperties.put("user",User)
    connectionProperties.put("password",Password)
    //保存dataframe到数据库
    SavaJdbc.Save(df12345,"jdbc:oracle:thin:@10.15.61.36:1521:jzpt","T_JF_TMP",connectionProperties)

    sc.stop()
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

