package com.triman.bigdata.syrk

/**
  * Created by hadoop on 2016/3/22.
  */
import java.text.SimpleDateFormat
import java.util.Date

import com.triman.bigdata.util._
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ListBuffer

object job2 {
  //methods to avoid NullPointerException
  def evl(x:Int):Int= {
    if (x==null) 0  else x
  }
  def getIntByFieldName(r:Row,name:String): Int =
  {
    try {
      val s=r.getAs[String](name)
      s.toInt
    }
    catch {
      case e:Exception => 0
    }
  }
  def evl2Int(x:String):Int= {
    if (x==null) 0
      else
        try{x.toInt} catch{
        case ex :Exception => 0}
    }
  def DoubleStr2Int(x:String):Int= {
    if (x==null) 0
    else
      try{x.toDouble.toInt} catch{
        case ex :Exception => 0}
  }
  def evl(x:String):String={
    if (x==null) "null" else x
  }
  def Any2Int(x:Any):Int ={
    var t = 0
    if ( x.isInstanceOf[String] ) {
      try {
        t= x.asInstanceOf[String].toInt
      }
      catch{
        case ex :Exception => t= 0
      }
    }
    if(x.isInstanceOf[Int]){
      t= x.asInstanceOf[Int]
    }
    t
  }
  case class Fw_wtsj1(var fwbm: String, var jcwdm: String, var pcsdm: String, var wzjz: Int,
                     var wrjz: Int, var lhhj: Int, var wsfzjz: Int,
                     var wcndj: Int, var drjz: Int, var tjsj: String, var zzjz: Int)

  val wcn = DateUtil.getWcn()
  val (tjsj,tjsj1,tjsj2,tjsj3) = DateUtil.getMonthB4()
  val (month_mod2,month_mod3) = DateUtil.getMonthMod()

  def main(args: Array[String]) {
    val (sc,sqlContext) = Initial_Spark.Initial_Spark("Job2",tjsj)

    import sqlContext.implicits._

    //定义用户函数 用于hiveql查询
    sqlContext.udf.register("EQ_Str", (arg1: String, arg2: String) =>  if (evl(arg2) == evl(arg1)) 1 else 0)
    sqlContext.udf.register("LS_Str", (arg1: String, arg2: String) => if (evl(arg2) > evl(arg1)) 1 else 0)
    sqlContext.udf.register("BG_Str", (arg1: String, arg2: String) => if (evl(arg2) < evl(arg1)) 1 else 0)
    sqlContext.udf.register("LK_Str", (arg1: String, arg2: String) => arg1+arg2 )
    sqlContext.udf.register("BG_Int", (arg1: Int, arg2: Int) => if (evl(arg2) < evl(arg1)) 1 else 0)
    sqlContext.udf.register("EQ_Int", (arg1: Int, arg2:Int ) => if (evl(arg2) == evl(arg1)) 1 else 0)
    sqlContext.udf.register("JS_Int", (arg1: Int, arg2:Int ) => if (evl(arg2)>=1 && evl(arg1)==0) 1 else 0)
    sqlContext.udf.register("Plus_NoZero", (arg1: Int, arg2:Int ) =>  if (evl(arg2) + evl(arg1)>0 ) 1 else 0)
    sqlContext.udf.register("Trim", (arg1:String) => arg1.trim )
    //
    //加载数据表
    val base = "hdfs://linux90:9000/user/hadoop/"
    val T_ZDFW_BD2 = sqlContext.read.parquet(base+tjsj2+"parquet/T_ZDFW_BD")
    T_ZDFW_BD2.registerTempTable("T_ZDFW_BD2")
    val T_ZDFW_BD3 = sqlContext.read.parquet(base+tjsj3+"parquet/T_ZDFW_BD")
    T_ZDFW_BD3.registerTempTable("T_ZDFW_BD3")
    val T_ZDFW_BD = sqlContext.read.parquet(base+tjsj+"parquet/T_ZDFW_BD")
    T_ZDFW_BD.registerTempTable("T_ZDFW_BD")

    //本月变动
    val zdfw_bd = sqlContext.sql("SELECT fwbm as FWBM1,pcsdm as PCSDM,jcwdm as JCWDM,tjsj as TJSJ,wcndj as WCNDJ ,wsfzjz as WSFZJZ,wrjz as WRJZ ,"+
      " wzjz as WZJZ,zzjz as ZZJZ ,lhhj as LHHJ, drjz as DRJZ FROM T_ZDFW_BD")
    //两个月前的
    val zdfw_bd2 = sqlContext.sql("SELECT  FWBM AS FWBM2 ,WRJZ AS WRJZS FROM T_ZDFW_BD2")
    //3个月前的
    val zdfw_bd3 = sqlContext.sql("SELECT  FWBM AS FWBM3 ,LHHJ AS LHHJS,DRJZ AS DRJZS FROM T_ZDFW_BD3")
    //半个月前的
    val jdbc =new JdbcConn
    val rec = jdbc.getZW_wtsj(tjsj1+"16")
    val zzwz = sc.parallelize(rec,24).toDF()
    // 本月+半个月前+两个月前+3个月前
    val thisjoin2=zdfw_bd.join(zdfw_bd2,zdfw_bd("FWBM1")===zdfw_bd2("FWBM2"),"left")
    val thisjoin2join3=thisjoin2.join(zdfw_bd3,thisjoin2("FWBM1")===zdfw_bd3("FWBM3"),"left")
    val x=thisjoin2join3
    val xjoinzzwz=x.join(zzwz,x("FWBM1")===zzwz("FWBM"),"left")
    //问题房屋数据统计
    //有的半个月（固定执行）
    //有个一个月（固定执行）
    //有的2个月（条件执行）
    //有的3个月（条件执行）
    //最后filter选取有用的数据
      val t_fw_wtsj =xjoinzzwz.map{
        r=>
          if (month_mod2==1 && month_mod3==1) {  fun2(r.getAs[String]("FWBM1"),r.getAs[String]("JCWDM"),r.getAs[String]("PCSDM"),r.getAs[String]("TJSJ")
          ,(r.getAs[Int]("WZJZ")),(r.getAs[Int]("WZJZS")),(r.getAs[Int]("ZZJZ")),(r.getAs[Int]("ZZJZS"))
          ,(r.getAs[Int]("WCNDJ")),(r.getAs[Int]("WSFZJZ"))
          ,(r.getAs[Int]("WRJZ")),(r.getAs[Int]("WRJZS"))
          ,(r.getAs[Int]("LHHJ")),(r.getAs[Int]("LHHJS")),(r.getAs[Int]("DRJZ")),(r.getAs[Int]("DRJZS")))}
        else if (month_mod2==1){  fun2(r.getAs[String]("FWBM1"),r.getAs[String]("JCWDM"),r.getAs[String]("PCSDM"),r.getAs[String]("TJSJ")
          ,(r.getAs[Int]("WZJZ")),(r.getAs[Int]("WZJZS")),(r.getAs[Int]("ZZJZ")),(r.getAs[Int]("ZZJZS"))
          ,(r.getAs[Int]("WCNDJ")),(r.getAs[Int]("WSFZJZ"))
          ,(r.getAs[Int]("WRJZ")),(r.getAs[Int]("WRJZS"))
          ,0,0,0,0)}
        else if  (month_mod3==1) {   fun2(r.getAs[String]("FWBM1"),r.getAs[String]("JCWDM"),r.getAs[String]("PCSDM"),r.getAs[String]("TJSJ")
          ,(r.getAs[Int]("WZJZ")),(r.getAs[Int]("WZJZS")),(r.getAs[Int]("ZZJZ")),(r.getAs[Int]("ZZJZS"))
          ,(r.getAs[Int]("WCNDJ")),(r.getAs[Int]("WSFZJZ"))
          ,0,0
          ,(r.getAs[Int]("LHHJ")),(r.getAs[Int]("LHHJS")),(r.getAs[Int]("DRJZ")),(r.getAs[Int]("DRJZS")))}
        else {
          fun2(r.getAs[String]("FWBM1"), r.getAs[String]("JCWDM"), r.getAs[String]("PCSDM"), r.getAs[String]("TJSJ")
            , (r.getAs[Int]("WZJZ")), (r.getAs[Int]("WZJZS")), (r.getAs[Int]("ZZJZ")), (r.getAs[Int]("ZZJZS"))
            , (r.getAs[Int]("WCNDJ")), (r.getAs[Int]("WSFZJZ"))
            , 0, 0
            , 0, 0, 0, 0)
        }
      }.toDF().filter("wrjz >= 1 or lhhj >= 1 or wsfzjz >= 1 or wcndj >= 1 or  drjz >= 1 or wzjz>=1 or zzjz >=1")
    //输出数据到oracle
    SavaJdbc.Save(t_fw_wtsj,"SYRK.T_FW_WTSJ")
    t_fw_wtsj.registerTempTable("T_FW_WTSJ")
    //读取字典表
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
    //统计要提交的信息
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
    //选取有用的信息
    tj_fw_wtsj2.filter("substr(pcsdm, 5, 2) in ( "+str.substring(0,str.length-1)+")").registerTempTable("tt")
    val tj_fw_wtsj=
      sqlContext.sql("select pcsdm,MC as pcsmc ,wzjz_y,wzjz_x,wrjz_y,wrjz_x,lhhj_y,lhhj_x,wsfzjz_y,wsfzjz_x,wcndj_y,wcndj_x,drjz_y,drjz_x,tjsj,"+
        "xj1,xj3,zzjz_y,zzjz_x,xj_wzz from tt")
   // tj_fw_wtsj.printSchema()
    //输出到oracle
    SavaJdbc.Save(tj_fw_wtsj.repartition(4),"SYRK.T_TJ_FW_WTSJ")
//    val cnt = tj_fw_wtsj.count
//    tj_fw_wtsj.orderBy("pcsdm").collect().foreach(println)
//    println(cnt)

    sc.stop()
  }

  def fun2 (fwbm:String,jcwdm: String,pcsdm:String,tjsj:String,
            wz:Int, wz_half:Int,zz:Int,zz_half:Int,
             wcn:Int,wsfz:Int,wr:Int,wr_2:Int,
            lhhj:Int,lhhj_3:Int,drjz:Int,drjz_3:Int):Fw_wtsj1 = {
    var v_wrjz = 0
    var v_lhhj = 0
    var v_wsfzjz = 0
    var v_wcndj = 0
    var v_drjz = 0
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
    //来沪无证人员居住（每个月）
    if ( wsfz == 3 ) v_wsfzjz = 1
    else v_wsfzjz = wsfz
    //来沪未成年独居（每个月）
    if ( wcn == 3 ) v_wcndj = 1
    else v_wcndj = wcn
    //无人居住房屋（每2个月）
    if ( wr > 0  && month_mod2 ==1 ) {
      if ( wr_2 >0  ) v_wrjz = 1
      else v_wrjz = 2
    }
    //来沪混居（个人房）（每三个月）
    if( lhhj >0 && month_mod3 ==1 ) {
      if ( lhhj_3 >0 ) v_lhhj = 1
      else v_lhhj = 2
    }
    //
    if( drjz >0 && month_mod3 ==1 ) {
      if ( drjz_3 >0 ) v_drjz = 1
      else v_drjz = 2
    }
    Fw_wtsj1( fwbm,  jcwdm ,  pcsdm ,  v_wzjz  ,v_wrjz,  v_lhhj ,  v_wsfzjz ,
      v_wcndj ,  v_drjz ,  tjsj  , v_zzjz)
  }
}





