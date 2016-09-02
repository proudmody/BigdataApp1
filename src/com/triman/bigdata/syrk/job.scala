package com.triman.bigdata.syrk

/**
  * 统计各个房屋中的人员信息
  */

import com.triman.bigdata.util.{DateUtil, Initial_Spark, SavaJdbc}

object job {
  val wcn = DateUtil.getWcn()
  val (tjsj,tjsj1,tjsj2,tjsj3) = DateUtil.getMonthB4()
  val (month_mod2,month_mod3) = DateUtil.getMonthMod()

  def main(args: Array[String]) {
    val (sc,sqlContext) = Initial_Spark.Initial_Spark("Job1",tjsj)
    import sqlContext.implicits._

    //加载数据表
    val base = "hdfs://linux90:9000/user/hadoop/"
    val T_RJBXX = sqlContext.read.parquet(base+tjsj+"parquet/T_RJBXX")
    T_RJBXX.registerTempTable("T_RJBXX")
    val T_FWJBXX = sqlContext.read.parquet(base+tjsj+"parquet/T_FWJBXX")
    T_FWJBXX.registerTempTable("T_FWJBXX")
    val T_WSFHMLHRY = sqlContext.read.parquet(base+tjsj+"parquet/T_WSFHMLHRY")
    T_WSFHMLHRY.registerTempTable("T_WSFHMLHRY")
    val T_ZDFW_BD1 = sqlContext.read.parquet(base+tjsj1+"parquet/T_ZDFW_BD")
    T_ZDFW_BD1.registerTempTable("T_ZDFW_BD1")
    val T_FWGLXX = sqlContext.read.parquet(base+tjsj+"parquet/T_FWGLXX")
    T_FWGLXX.registerTempTable("T_FWGLXX")

    //定义用户函数 用于hiveql查询
    sqlContext.udf.register("Null2Zero", (arg1: Any) => Any2Int(arg1))
    sqlContext.udf.register("EQ_Str", (arg1: String, arg2: String) => if (evl(arg2) == evl(arg1)) 1 else 0)
    sqlContext.udf.register("LS_Str", (arg1: String, arg2: String) => if (evl(arg2) > evl(arg1)) 1 else 0)
    sqlContext.udf.register("BG_Str", (arg1: String, arg2: String) => if (evl(arg2) < evl(arg1)) 1 else 0)
    sqlContext.udf.register("BG_Str_age", (arg1: String, arg2: String,arg3: String) => if ((evl(arg2) < evl(arg1)) && (arg3 =="02")) 1 else 0)
    sqlContext.udf.register("LK_Str", (arg1: String, arg2: String) => arg1 + arg2)
    sqlContext.udf.register("BG_Int", (arg1: Int, arg2: Int) => if (evl(arg2) < evl(arg1)) 1 else 0)
    sqlContext.udf.register("EQ_Int", (arg1: Int, arg2: Int) => if (evl(arg2) == evl(arg1)) 1 else 0)
    sqlContext.udf.register("JS_Int", (arg1: Any, arg2: Int ) => if (evl(arg2) >= 1 && Any2Int(arg1)==0) 1 else 0)
    sqlContext.udf.register("Plus_NoZero", (arg1: Int, arg2: Int) => if (evl(arg2) + evl(arg1) > 0) 1 else 0)
    sqlContext.udf.register("Trim", (arg1: String) => arg1.trim)
    //本月人基本信息按房汇总
    val t_ls_fwid = sqlContext.sql("SELECT NVL(JZFWID,HJFWID) AS FWID, COUNT(1) AS ZS ," +
      "SUM(EQ_STR(SYRKLBDM,'01')) AS HJ ,SUM(EQ_STR(SYRKLBDM,'02')) AS LH ,SUM(EQ_STR(SYRKLBDM,'03')) AS JW," +
      "SUM(EQ_STR(MZDM,'05')) AS WZ,SUM(EQ_STR(MZDM,'04')) AS ZZ," +
      "SUM(BG_Str_age(CSRQ,'" + wcn + "',SYRKLBDM)) AS WCN" +
      " FROM T_RJBXX GROUP BY(NVL(JZFWID,HJFWID))")
    //本月人基本信息按房汇总 + 房屋管理信息
    val curlist = T_FWGLXX.join(t_ls_fwid, t_ls_fwid("FWID") === T_FWGLXX("FWBM"), "left")
    curlist.registerTempTable("CURLIST")
    //选取有用字段
    val temp = sqlContext.sql("SELECT FWBM,JCWDM,PCSDM,Null2Zero(WZ) as WZ,Null2Zero(ZZ) as ZZ,Null2Zero(LH) as LH" +
      ",Null2Zero(WCN) as WCN ,Null2Zero(ZS) as ZS," +
      " JZLX, PCSMC,JCWMC FROM CURLIST")
    //获取无身份证人员信息
    val wsfjz = sqlContext.sql("SELECT FWBM AS FWBM2,SUM(LS_Str(CSRQ,'19980101')) AS WSFZ FROM T_WSFHMLHRY  GROUP BY FWBM").filter("WSFZ >0")
    val zdfwjoin1 = temp.join(wsfjz, wsfjz("FWBM2") === temp("FWBM"), "left")
    zdfwjoin1.registerTempTable("TEMP")
    //本月变动信息
    val zdfwjoin = sqlContext.sql("SELECT FWBM,JCWDM,PCSDM,Null2Zero(WZ) as WZ,Null2Zero(ZZ) as ZZ,Null2Zero(LH) as LH" +
      ",Null2Zero(WCN) as WCN ,Null2Zero(ZS) as ZS,Null2Zero(WSFZ) as WSFZ," +
      " JZLX, PCSMC,JCWMC FROM TEMP")
    //上月变动统计信息
    val last_bd = sqlContext.sql("SELECT pcsdm AS dm, wzjz AS WZJZS,wrjz AS WRJZS,lhhj AS LHHJS,wsfzjz AS WSFZJZS" +
      ",wcndj AS WCNDJS,drjz AS DRJZS,(tjsj) AS TJSJS,zzjz AS ZZJZS,(zs) AS ZSS ,(fwbm) AS FWBM3 FROM T_ZDFW_BD1").cache()
    val zdfwJoinWsfzjoinZdfwS = zdfwjoin.join(last_bd, last_bd("FWBM3") === zdfwjoin("FWBM"), "left")
    // 本月信息+房屋名称
    val fwmc = sqlContext.sql("SELECT FWBM as FWBM4,LK_STR(MLPHXX , SH) AS FWMC FROM T_FWJBXX")
    val zdfwJoinWsfzjoinZdfwSJoinJzlx = zdfwJoinWsfzjoinZdfwS.join(fwmc, zdfwJoinWsfzjoinZdfwS("FWBM") === fwmc("FWBM4"), "left")

    zdfwJoinWsfzjoinZdfwSJoinJzlx.na.fill(0.0)
    //统计变动结果
    val zdfw_bd = zdfwJoinWsfzjoinZdfwSJoinJzlx.map {
      r => val x = fun1(r.getAs[String]("FWBM"), r.getAs[String]("JZLX"), r.getAs[String]("PCSDM"), r.getAs[String]("JCWDM"), r.getAs[String]("FWMC"),
        r.getAs[String]("PCSMC"), r.getAs[String]("JCWMC"), tjsj, Any2Int(r.getAs[Any]("ZS")),
        Any2Int(r.getAs[Any]("ZSS")), Any2Int(r.getAs[Any]("WZ")), Any2Int(r.getAs[Any]("WZJZS")), Any2Int(r.getAs[Any]("ZZ")), Any2Int(r.getAs[Any]("ZZJZS")),
        Any2Int(r.getAs[Any]("WCN")), Any2Int(r.getAs[Any]("WCNDJS")), Any2Int(r.getAs[Any]("LH")), Any2Int(r.getAs[Any]("DRJZS")), Any2Int(r.getAs[Any]("LHHJS")),
        Any2Int(r.getAs[Any]("WRJZS")), Any2Int(r.getAs[Any]("WSFZ")), Any2Int(r.getAs[Any]("WSFZJZS")))
        x
    }.toDF().filter("wzjz >= 1 or wrjz >= 1 or lhhj >= 1 or wsfzjz >= 1 or zzjz >= 1 or wcndj >= 1 or drjz >= 1")
    //输出 作为下个月输入的上月变动统计信息
    zdfw_bd.write.parquet(tjsj+"parquet/T_ZDFW_BD")
    //输出到oracle
    SavaJdbc.Save(zdfw_bd,"SYRK.T_ZDFW_BD")
    zdfw_bd.registerTempTable("ZDFWBD")

    //  上个月统计信息left join 本月统计信息
    val last_bd_join_zdfw_bd = last_bd.join(zdfw_bd, last_bd("FWBM3") === zdfw_bd("fwbm"), "left").cache
    last_bd_join_zdfw_bd.registerTempTable("BDJOINBD")

    //计算本月减少数据 上个月统计信息left join 本月统计信息 则 上月有 本月无 可以推出减少；
    val tj_Zdfw_bd_js = sqlContext.sql("SELECT dm ," +
      "sum(JS_Int(wzjz,WZJZS)) as wzjz_js ," +
      "sum(JS_Int(zzjz,ZZJZS)) as zzjz_js ," +
      "sum(JS_Int(wcndj,WCNDJS)) as wcndj_js ," +
      "sum(JS_Int(wsfzjz,WSFZJZS)) as wsfzjz_js ," +
      "sum(JS_Int(wrjz,WRJZS)) as wrjz_js ," +
      "sum(JS_Int(lhhj,LHHJS)) as lhhj_js ," +
      "sum(JS_Int(drjz,DRJZS)) as drjz_js " +
      " FROM BDJOINBD GROUP BY dm ")
    //计算本月增加数据 本月统计信息left join上个月统计信息 则 本月有 上月无 可以推出新增；
    val tj_Zdfw_bd_nojs = sqlContext.sql("SELECT first(tjsj) as tjsj, pcsdm ," +
      "sum(BG_Int(wzjz,0)) as wzjz_zs ," + "sum(EQ_Int(wzjz,2)) as wzjz_xz ," +
      "sum(EQ_Int(wzjz,3)) as wzjz_bd ," +
      "sum(BG_Int(zzjz,0)) as zzjz_zs ," + "sum(EQ_Int(zzjz,2)) as zzjz_xz ," +
      "sum(EQ_Int(zzjz,3)) as zzjz_bd ," +
      "sum(BG_Int(wcndj,0)) as wcndj_zs ," + "sum(EQ_Int(wcndj,2)) as wcndj_xz ," +
      "sum(EQ_Int(wcndj,3)) as wcndj_bd ," +
      "sum(BG_Int(wsfzjz,0)) as wsfzjz_zs ," + "sum(EQ_Int(wsfzjz,2)) as wsfzjz_xz ," +
       "sum(EQ_Int(wsfzjz,3)) as wsfzjz_bd ," +
      "sum(BG_Int(wrjz,0)) as wrjz_zs ," + "sum(EQ_Int(wrjz,2)) as wrjz_xz ," +
      "sum(EQ_Int(wrjz,3)) as wrjz_bd ," +
      "sum(BG_Int(lhhj,0)) as lhhj_zs ," + "sum(EQ_Int(lhhj,2)) as lhhj_xz ," +
      "sum(EQ_Int(lhhj,3)) as lhhj_bd ," +
      "sum(BG_Int(drjz,0)) as drjz_zs ," + "sum(EQ_Int(drjz,2)) as drjz_xz ," +
      "sum(EQ_Int(drjz,3)) as drjz_bd " +
      " FROM ZDFWBD GROUP BY pcsdm ")
    //读取字典表
    val zdpcs = sqlContext.load("jdbc", Map(
      "url" -> "",
      "dbtable" -> "d_qx_sh",
      "user" -> "",
      "password" -> "",
      "fetchSize" -> "100",
      "driver" -> "oracle.jdbc.driver.OracleDriver"))
    //创建一个逗号隔开的字符串如‘1，2，3，4’ 用于sql中的 in （‘1，2，3，4’）语句
    val pcsdm = zdpcs.map{
      r => r.getAs[String]("DM12")
    }.collect()
    var str = ""
    for(p<-pcsdm){
      str+="'"+p+"',"
    }
    //房屋变动汇总 包含新增和减少的信息
    val tj_Zdfw_bd = tj_Zdfw_bd_nojs.join(tj_Zdfw_bd_js,tj_Zdfw_bd_js("dm")===tj_Zdfw_bd_nojs("pcsdm")).repartition(4)
    val tj_Zdfw_bd_s = tj_Zdfw_bd.select("tjsj","pcsdm","wzjz_zs","wzjz_xz","wzjz_js","wzjz_bd","zzjz_zs","zzjz_xz","zzjz_js","zzjz_bd",
      "wcndj_zs","wcndj_xz","wcndj_js","wcndj_bd","wsfzjz_zs","wsfzjz_xz","wsfzjz_js","wsfzjz_bd",
      "wrjz_zs","wrjz_xz","wrjz_js","wrjz_bd","lhhj_zs","lhhj_xz","lhhj_js","lhhj_bd",
      "drjz_zs","drjz_xz","drjz_js","drjz_bd"
    ).filter("substr(pcsdm, 5, 2) in ( "+str.substring(0,str.length-1)+")")
    //写入oracle数据库
    SavaJdbc.Save(tj_Zdfw_bd_s.repartition(4),"SYRK.T_TJ_ZDFW_BD")
   // tj_Zdfw_bd_s.collect().foreach(println)
   //valtj_Zdfw_bd.count())
    return

  }
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
    if (x==null) return t
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
                     var fwmc: String, var pcsmc: String, var jcwmc: String, val wrjz_wbd:String =null)

  //GroupBy PCSDM
  class Tj_zdfw_bd(tjsj: String, pcsdm: String,
                   wzjz_zs: Long, wzjz_xz: Long, wzjz_js: Long, wzjz_bd: Long,
                   zzjz_zs: Long, zzjz_xz: Long, zzjz_js: Long, zzjz_bd: Long,
                   wcndj_zs: Long, wcndj_xz: Long, wcndj_js: Long, wcndj_bd: Long,
                   wsfzjz_zs: Long, wsfzjz_xz: Long, wsfzjz_js: Long, wsfzjz_bd: Long,
                   wrjz_zs: Long, wrjz_xz: Long, wrjz_js: Long, wrjz_bd: Long,
                   lhhj_zs: Long, lhhj_xz: Long, lhhj_js: Long, lhhj_bd: Long,
                   drjz_zs: Long, drjz_xz: Long, drjz_js: Long, drjz_bd: Long
                  ) extends Serializable {}

  //
  case class Fw_wtsj(var fwbm: String, var jcwdm: String, var pcsdm: String, var wzjz: Int,
                     var zzjz: Int, var wrjz: Int, var lhhj: Int, var wsfzjz: Int,
                     var wcndj: Int, var drjz: Int, var tjsj: String)

  //bd_fenxi
  def fun1(fwbm: String, JZLX: String, pcsdm: String, jcwdm: String, fwmc: String, pcsmc: String, jcwmc: String, tjsj: String,
           zs: Int, zs_s: Int, wz: Int, wz_flag_s: Int, zz: Int, zz_flag_s: Int, wcn: Int, wcn_flag_s: Int,
           lh: Int, drjz_flag_s: Int, lhhj_flag_s: Int, wrjz_flag_s: Int, wsfz: Int, wsfzjz_flag_s: Int): Zdfw_bd = {
    //wzjz
    var wzjz = 0
    if (wz > 0)
      if (wz_flag_s == 0) wzjz = 2
      else if (zs == zs_s) wzjz = 1
      else wzjz = 3
    //zzjz
    var zzjz = 0
    if (zz > 0)
      if (zz_flag_s == 0) zzjz = 2
      else if (zs == zs_s) zzjz = 1
      else zzjz = 3
    //lhwcnrdr
    var wcndj = 0
    if (JZLX == "01" && wcn == 1 && zs == 1)
      if (wcn_flag_s == 0) wcndj = 2
      else if (zs == zs_s) wcndj = 1
      else wcndj = 3
    //lh>=10jz
    var drjz = 0
    if (JZLX == "01" && lh > 10)
      if (drjz_flag_s == 0) drjz = 2
      else if (zs == zs_s) drjz = 1
      else drjz = 3
    //lhhj
    var lhhj = 0
    if (JZLX == "01" && lh > 0 && zs > lh)
      if (lhhj_flag_s == 0) lhhj = 2
      else if (zs == zs_s) lhhj = 1
      else lhhj = 3
    //wrjz
    var wrjz = 0
    if (JZLX == "01" && zs == 0)
      if (wrjz_flag_s == 0) wrjz = 2
      else if (zs == zs_s) wrjz = 1
      else wrjz = 3
    //wsfzjz
    var wsfzjz = 0
    if (wsfz > 0)
      if (wsfzjz_flag_s == 0) wsfzjz = 2
      else if (zs == zs_s) wsfzjz = 1
      else wsfzjz = 3

    Zdfw_bd(fwbm, jcwdm, pcsdm, wzjz, wrjz, lhhj, wsfzjz, wcndj, drjz, tjsj,zzjz, zs, fwmc, pcsmc, jcwmc)
  }
}




