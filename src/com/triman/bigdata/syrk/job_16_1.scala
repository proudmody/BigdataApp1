package com.triman.bigdata.syrk

/**
  * Created by hadoop on 2016/3/22.
  */
import java.text.SimpleDateFormat
import java.util.Date

import com.triman.bigdata.util.{DateUtil, HDFSUtil, Initial_Spark, SavaJdbc}
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

object job_16_1 {
  //methods to avoid NullPointerException
  def evl(x: Int): Int = {
    if (x == null) 0 else x
  }

  def evl2Int(x: String): Int = {
    if (x == null) 0
    else
      try {
        x.toInt
      } catch {
        case ex: Exception => 0
      }
  }

  def DoubleStr2Int(x: String): Int = {
    if (x == null) 0
    else
      try {
        x.toDouble.toInt
      } catch {
        case ex: Exception => 0
      }
  }

  def evl(x: String): String = {
    if (x == null) "null" else x

  }

  def Any2Int(x: Any): Int = {
    var t = 0
    if (x == null) return t
    if (x.isInstanceOf[String]) {
      try {
        t = x.asInstanceOf[String].toInt
      }
      catch {
        case ex: Exception => t = 0
      }
    }
    if (x.isInstanceOf[Int]) {
      t = x.asInstanceOf[Int]
    }
    if (x.isInstanceOf[Long]) {
      t = x.asInstanceOf[Long].toInt
    }
    if (x.isInstanceOf[Double]) {
      t = x.asInstanceOf[Double].toInt
    }
    t
  }

  //GroupBy fwdm
  case class Zdfw_bd(var fwbm: String, var jcwdm: String, var pcsdm: String, var wzjz: Int,
                     var wrjz: Int, var lhhj: Int, var wsfzjz: Int,
                     var wcndj: Int, var drjz: Int, var tjsj: String, var zzjz: Int, var zs: Int,
                     var fwmc: String, var pcsmc: String, var jcwmc: String, val wrjz_wbd: String = null)

  //
  case class Fw_wtsj(var fwbm: String, var jcwdm: String, var pcsdm: String, var wzjz: Int,
                     var wrjz: Int, var lhhj: Int, var wsfzjz: Int,
                     var wcndj: Int, var drjz: Int, var tjsj: String, var zzjz: Int)


  val wcn = DateUtil.getWcn()
  val (tjsj0,tjsj1,tjsj2,tjsj3) = DateUtil.getMonthB4()
  val (month_mod2,month_mod3) = DateUtil.getMonthMod()
  val tjsj = tjsj0 + "16"
  val tjsjs = tjsj0

  def main(args: Array[String]) {
    val (sc,sqlContext) = Initial_Spark.Initial_Spark("Job_16_1",tjsj)
    import sqlContext.implicits._

    //加载数据表
    val base = "hdfs://linux90:9000/user/hadoop/"
    val T_RJBXX = sqlContext.read.parquet(base + tjsj + "parquet/T_RJBXX")
    T_RJBXX.registerTempTable("T_RJBXX")
    val T_FWJBXX = sqlContext.read.parquet(base + tjsj + "parquet/T_FWJBXX")
    T_FWJBXX.registerTempTable("T_FWJBXX")
    val T_ZDFW_BD = sqlContext.read.parquet(base + tjsjs + "parquet/T_ZDFW_BD")
    T_ZDFW_BD.registerTempTable("T_ZDFW_BD")

    //定义用户函数 用于hiveql查询
    sqlContext.udf.register("Null2Zero", (arg1: Any) => Any2Int(arg1))
    sqlContext.udf.register("EQ_Str", (arg1: String, arg2: String) => if (evl(arg2) == evl(arg1)) 1 else 0)
    sqlContext.udf.register("LS_Str", (arg1: String, arg2: String) => if (evl(arg2) > evl(arg1)) 1 else 0)
    sqlContext.udf.register("BG_Str", (arg1: String, arg2: String) => if (evl(arg2) < evl(arg1)) 1 else 0)
    sqlContext.udf.register("BG_Str_age", (arg1: String, arg2: String, arg3: String) => if ((evl(arg2) < evl(arg1)) && (arg3 == "02")) 1 else 0)
    sqlContext.udf.register("LK_Str", (arg1: String, arg2: String) => arg1 + arg2)
    sqlContext.udf.register("BG_Int", (arg1: Int, arg2: Int) => if (evl(arg2) < evl(arg1)) 1 else 0)
    sqlContext.udf.register("EQ_Int", (arg1: Int, arg2: Int) => if (evl(arg2) == evl(arg1)) 1 else 0)
    sqlContext.udf.register("JS_Int", (arg1: Any, arg2: Int) => if (evl(arg2) >= 1 && Any2Int(arg1) == 0) 1 else 0)
    sqlContext.udf.register("Plus_NoZero", (arg1: Int, arg2: Int) => if (evl(arg2) + evl(arg1) > 0) 1 else 0)
    sqlContext.udf.register("Trim", (arg1: String) => arg1.trim)
    //

    //
    val t_ls_fwid = sqlContext.sql("SELECT NVL(JZFWID,HJFWID) AS FWID," +
      "SUM(EQ_STR(MZDM,'05')) AS WZJZ,SUM(EQ_STR(MZDM,'04')) AS ZZJZ" +
      " FROM T_RJBXX GROUP BY(NVL(JZFWID,HJFWID))").filter(" WZJZ>=1 or ZZJZ >=1")

    //this month -> fwjbxx.Join(Rjbxx)
    val curlist = T_FWJBXX.join(t_ls_fwid, t_ls_fwid("FWID") === T_FWJBXX("FWBM"), "left")
    curlist.registerTempTable("CURLIST")
    //
    val temp = sqlContext.sql("SELECT FWBM,JCWDM,PCSDM,WZJZ, ZZJZ ," +
      "PCSMC,JCWMC FROM CURLIST")
    //去除null

    val zdfw_bd = sqlContext.sql("SELECT fwbm as FWBM1," +
      " wzjz as WZJZS,zzjz as ZZJZS FROM T_ZDFW_BD")

    val xjoinzzwz=temp.join(zdfw_bd,temp("FWBM")===zdfw_bd("FWBM1"),"left")

    val t_fw_wtsj =xjoinzzwz.map{
      r=>
          fun2(r.getAs[String]("FWBM"), r.getAs[String]("JCWDM"), r.getAs[String]("PCSDM"), tjsj
            , (Any2Int(r.getAs[Any]("WZJZ"))), (Any2Int(r.getAs[Any]("WZJZS"))), (Any2Int(r.getAs[Any]("ZZJZ"))), (Any2Int(r.getAs[Any]("ZZJZS")))
            , 0, 0
            , 0, 0
            , 0, 0, 0, 0)

    }.toDF().filter(" wzjz>=1 or zzjz >=1")
    SavaJdbc.Save(t_fw_wtsj.repartition(4),"SYRK.T_FW_WTSJ")
    t_fw_wtsj.registerTempTable("T_FW_WTSJ")

    val mcdf = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:oracle:thin:@10.15.58.19:1521:syrk1",
      "dbtable" -> "fwgl.d_jg",
      "user" -> "syrk",
      "password" -> "smc@23977",
      "fetchSize" -> "100",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))
    val zdpcs = sqlContext.load("jdbc", Map(
      "url" -> "jdbc:oracle:thin:@10.15.58.19:1521:syrk1",
      "dbtable" -> "d_qx_sh",
      "user" -> "syrk",
      "password" -> "smc@23977",
      "fetchSize" -> "100",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))

    val pcsdm = zdpcs.map{
      r => r.getAs[String]("DM12")
    }.collect()
    var str = ""
    for(p<-pcsdm){
      str+="'"+p+"',"
    }

    //table ! final!
    val tj_fw_wtsj_1 = sqlContext.sql("SELECT pcsdm, " +
      "sum(EQ_Int(wzjz,1)) as wzjz_y ,"+"sum(EQ_Int(wzjz,2)) as wzjz_x ,"+
      "sum(EQ_Int(wrjz,1)) as wrjz_y ,"+"sum(EQ_Int(wrjz,2)) as wrjz_x ,"+
      "sum(EQ_Int(lhhj,1)) as lhhj_y ,"+"sum(EQ_Int(lhhj,2)) as lhhj_x ,"+
      "sum(EQ_Int(wsfzjz,1)) as wsfzjz_y ,"+"sum(EQ_Int(wsfzjz,2)) as wsfzjz_x ,"+
      "sum(EQ_Int(wcndj,1)) as wcndj_y ,"+"sum(EQ_Int(wcndj,2)) as wcndj_x ,"+
      "sum(EQ_Int(drjz,1)) as drjz_y ,"+"sum(EQ_Int(drjz,2)) as drjz_x,"+
      " first(tjsj) as tjsj,"+
      "sum(Plus_NoZero(wcndj,wsfzjz)) as xj1 ,"+
      "sum(Plus_NoZero(drjz,lhhj)) as xj3 ,"+
      "sum(EQ_Int(zzjz,1)) as zzjz_y ,"+ "sum(EQ_Int(zzjz,2)) as zzjz_x ,"+
      "sum(Plus_NoZero(zzjz,wzjz)) as xj_wzz "+
      "FROM T_FW_WTSJ t GROUP BY pcsdm ").repartition(4)
    val tj_fw_wtsj2=tj_fw_wtsj_1.join(mcdf,mcdf("DM")===tj_fw_wtsj_1("pcsdm"),"left")
    tj_fw_wtsj2.filter("substr(pcsdm, 5, 2) in ( "+str.substring(0,str.length-1)+")").registerTempTable("tt")
    val tj_fw_wtsj=
      sqlContext.sql("select pcsdm,MC as pcsmc ,wzjz_y,wzjz_x,wrjz_y,wrjz_x,lhhj_y,lhhj_x,wsfzjz_y,wsfzjz_x,wcndj_y,wcndj_x,drjz_y,drjz_x,tjsj,"+
        "xj1,xj3,zzjz_y,zzjz_x,xj_wzz from tt")
    // tj_fw_wtsj.printSchema()
    SavaJdbc.Save(tj_fw_wtsj.repartition(4),"SYRK.T_TJ_FW_WTSJ")
//        val cnt = tj_fw_wtsj.count
//        tj_fw_wtsj.orderBy("pcsdm").collect().foreach(println)
//        println(cnt)


    return

  }
  def fun2 (fwbm:String,jcwdm: String,pcsdm:String,tjsj:String,
            wz:Int, wz_half:Int,zz:Int,zz_half:Int,
            wcn:Int,wsfz:Int,wr:Int,wr_2:Int,
            lhhj:Int,lhhj_3:Int,drjz:Int,drjz_3:Int):Fw_wtsj = {
    val v_wrjz = 0
    val v_lhhj = 0
    val v_wsfzjz = 0
    val v_wcndj = 0
    val v_drjz = 0
    var v_wzjz = 0
    var v_zzjz = 0
    //wz
    if ( wz > 0 ){
      if ( wz_half >= 1 ) v_wzjz = 1
      else v_wzjz = 2
    }
    //zz
    if ( zz > 0 ){
      if ( zz_half >= 1 ) v_zzjz = 1
      else v_zzjz = 2
    }
    Fw_wtsj( fwbm,  jcwdm ,  pcsdm ,  v_wzjz  ,v_wrjz,  v_lhhj ,  v_wsfzjz ,
      v_wcndj ,  v_drjz ,  tjsj  , v_zzjz)
  }
}