package com.triman.bigdata.jf

/**
  * 重点人员入室盗窃
  */
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import com.triman.bigdata.util.Initial_Spark
import org.apache.spark.sql.{DataFrame, SQLContext}
object P_PQLB_1 {
  def Any2Double(x: Any): Double = {
    var t = 0.0
    if (x==null) return t
    if (x.isInstanceOf[java.math.BigDecimal]) t = x.asInstanceOf[java.math.BigDecimal].doubleValue()
    if (x.isInstanceOf[Long]) t = x.asInstanceOf[Long].toDouble
    t
  }
  def Any2Int(x: Any): Int = {
    var t = 0
    if (x==null) return t
    if (x.isInstanceOf[Long]) t = x.asInstanceOf[Long].toInt
    t
  }
  def StrNoNull(x:String):String={
    if (x==null) "null"
    else x
  }
  def compute(sqlContext:SQLContext,T_ZDRY_JBXX:DataFrame,HX_A_AJJBQK:DataFrame,VW_WSBA_HX_R_XYRC:DataFrame
              ,T_WB_TRACE:DataFrame,T_RB_SHBX:DataFrame,V_JSZ:DataFrame,T_JDC:DataFrame):DataFrame = {
    import sqlContext.implicits._
    T_WB_TRACE.registerTempTable("T_WB_TRACE")
    //两周内跨越区数量
    val crossAreaCnt =  sqlContext.sql("SELECT ID_CODE AS ID_CODE1,COUNT(DISTINCT AREA_CODE) AS CROSSAREACNT FROM T_WB_TRACE GROUP BY ID_CODE")
                        .filter("CROSSAREACNT >= 3")
    //两周内单日凌晨上机
    val lateNightWB= sqlContext.sql("SELECT * FROM T_WB_TRACE WHERE SUBSTR(LOGIN_AT,9,2) IN ('00','01','02','03','04','05')")
    lateNightWB.registerTempTable("lateNightWB")
    val lateNightWBCnt = sqlContext.sql("select ID_CODE AS ID_CODE2,count(*) as NIGHTCNT from lateNightWB group by ID_CODE,SUBSTR(LOGIN_AT, 0, 8)")
      .filter("NIGHTCNT >=3").dropDuplicates(Array("ID_CODE2"))
    //前科数据
    val pqlbQk = HX_A_AJJBQK.rdd.filter{
      r=>
        val zabmc = StrNoNull(r.getAs[String]("ZABMC"))
        val ajztdm = StrNoNull(r.getAs[String]("AJZTDM"))
        if ((zabmc.contains("拎包")||zabmc.contains("扒窃"))&&((ajztdm=="204")||(ajztdm=="299"))) true
        else false
    }.map{
      r=>
        val guid =  r.getAs[String]("GUID")
        val ajbh =  r.getAs[String]("AJBH1")
        Qk1(guid,ajbh)
    }.toDF()
    val qk = pqlbQk.join(VW_WSBA_HX_R_XYRC,VW_WSBA_HX_R_XYRC("AJBH2")===pqlbQk("AJBH1"),"left")
    qk.registerTempTable("QK")
    val qk_cnt = sqlContext.sql("SELECT SFZHM,COUNT(AJBH1) as QKCNT FROM QK  GROUP BY SFZHM")
    //地域数据
    val Area = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:oracle:thin:@10.15.61.36:1521:jzpt",
      "dbtable" -> "d_zdry_gwdy_pqlb",
      "user" -> " priapweb",
      "password" -> "priapweb",
      "fetchSize" -> "0",
      "driver" -> "oracle.jdbc.driver.OracleDriver")).selectExpr("DM","SCORE")
    val tJoinArea = T_ZDRY_JBXX.join(Area,Area("DM")===T_ZDRY_JBXX("JG"),"left")
    val tJoinAreaJoinQk = tJoinArea.join(qk_cnt,tJoinArea("ZJHM")===qk_cnt("SFZHM"),"left")
    val tJoinAreaJoinQkJoinWb = tJoinAreaJoinQk.join(lateNightWBCnt,tJoinAreaJoinQk("ZJHM")===lateNightWBCnt("ID_CODE2"),"left").
                                  join(crossAreaCnt,tJoinAreaJoinQk("ZJHM")===crossAreaCnt("ID_CODE1"),"left")

    val tJoinAreaJoinQkJoinWbJoinSb = tJoinAreaJoinQkJoinWb.join(T_RB_SHBX,tJoinAreaJoinQkJoinWb("ZJHM")===T_RB_SHBX("SBZJHM"),"left")

    //    tJoinAreaJoinQkJoinWbJoinSb.filter("SBZJHM is null").head(100).foreach(println)
    //    tJoinAreaJoinQkJoinWbJoinSb.printSchema()
    //用map进行积分计算
    //mapPartitions本来是准备在数据分区上面用jdbc链接读取数据的，后来发现生产库select效率低，不如把数据导出来join
    val res = tJoinAreaJoinQkJoinWbJoinSb.mapPartitions{
      Partition=>{
        Partition.map{
             p=>
               val zjhm = p.getAs[String]("ZJHM")
               val scoreAge = getAgeScore(getAge(p.getAs[String]("CSRQ")))
               val scoreSex = getSexScore(p.getAs[String]("XB"))
               val scoreArea = Any2Double(p.getAs[Any]("SCORE"))
               val scoreQK = getQKScore(Any2Double(p.getAs[Any]("QKCNT")))
               val scoreDynamc = getDynamcScore(Any2Int(p.getAs[Any]("NIGHTCNT")),Any2Int(p.getAs[Any]("CROSSAREACNT")),p.getAs[Any]("SBZJHM"))
               val scoreTotal = scoreSex * (scoreAge + scoreArea + scoreQK ) * scoreDynamc;
               //Score4(scoreAge,scoreSex,scoreArea,scoreQK,scoreDynamc,scoreTotal)
               Score(zjhm,scoreQK,scoreTotal)
          }
        }
    }
    //res.toDF().orderBy($"S_total".desc).filter("S_qk > 0 or S_area > 0").head(100).foreach(println)
    res.toDF()
  }
  case class Qk1(GUID:String,AJBH1:String)
  case class Score4(S_age:Double,S_sex:Double,S_area:Double,S_qk:Double,S_dynamc:Double,S_total:Double)
  case class Score(ZJHM1:String,SQK1:Double,STOTAL1:Double)
  //用于统计的决策函数
  def getDynamcScore(nightCnt:Int,crossAreaCnt:Int,sbzjhm:Any):Double = {
    var vScore_dynamc =0.0
    if ( nightCnt >= 3 ) vScore_dynamc += 3
    if ( crossAreaCnt >=3 ) vScore_dynamc += 2
    if (( nightCnt >=3 )&&( crossAreaCnt >=3 )) vScore_dynamc += 10
    if ( sbzjhm != null ) vScore_dynamc += (-50)
    vScore_dynamc
  }
  def getAge(csrq: String): Int = {
    val now = new Date()
    val df1 = new SimpleDateFormat("yyyyMMdd")
    val birthday = df1.parse(csrq)
    val age = (now.getYear -birthday.getYear) * 12 + (now.getMonth -birthday.getMonth)
    return math.floor(age/12.0).toInt
  }
  def getAgeScore(age:Int):Double ={
    if ((age >=0) && (age<=15)) 1.0
    else if ((age >=16) && (age<=17)) 2.0
    else if ((age >=18) && (age<=25)) 10.0
    else if ((age >=26) && (age<=35)) 8.0
    else if ((age >=36) && (age<=44)) 5.0
    else if ((age >=45) && (age<=79)) 3.0
    else if ((age >=80) && (age<=200)) 0.0
    else 0.0
  }
  def getSexScore(xb: String):Double ={
    if (xb =="1") 1.0
    else 0.2
  }
  def getQKScore(qkcnt:Double):Double ={
    5.0*qkcnt
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

