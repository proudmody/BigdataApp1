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

object DataProcess{

  val wcn = DateUtil.getWcn()
  val (tjsj,tjsj1,tjsj2,tjsj3) = DateUtil.getMonthB4()
  val (month_mod2,month_mod3) = DateUtil.getMonthMod()

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

  def DoubleStr2Int(x: String): Int = {
    if (x == null || x.length==0) 0
    else
      try {
        x.toDouble.toInt
      } catch {
        case ex: Exception => 0
      }
  }

  def main(args: Array[String]) {

    val (sc,sqlContext) = Initial_Spark.Initial_Spark("DataProcess",tjsj)

    val base = "hdfs://linux90:9000/user/hadoop/"
    val T_ZDRY_JBXX = sqlContext.read.parquet(base+tjsj+"parquet/T_ZDRY_JBXX_OLD").repartition(10)
    T_ZDRY_JBXX.write.parquet("tjsj+parquet/T_ZDRY_JBXX_OLD1")
//    T_ZDFW_BD.foreachPartition{
//
//      partirion =>
//        Class.forName("oracle.jdbc.driver.OracleDriver"); // 加载Oracle驱动程序
//        //System.out.println("开始尝试连接数据库！")
//        //        val url = "jdbc:oracle:thin:@10.15.58.19:1521:syrk1"
//        //        val user = "syrk"
//        //        val password = "smc@23977";
//        val url = "jdbc:oracle:thin:@10.16.163.41:1521:ORCL"
//        val user = "zhyy"
//        val password = "triman";
//        val con = DriverManager.getConnection(url, user, password); // 获取连接
//        var i=1
//        val stmt = con.createStatement()
//        partirion.foreach{
//          p=>
//            val sql = "insert into T_ZDFW_BD values("+"'"+p.getString(0)+"',"+"'"+p.getString(1)+"',"+"'"+p.getString(2)+"',"+"'"+p.getInt(3)+"',"+
//              "'"+p.getInt(4)+"',"+"'"+p.getInt(5)+"',"+"'"+p.getInt(6)+"',"+"'"+p.getInt(7)+"',"+"'"+p.getInt(8)+"',"+"'"+p.getString(9)+"',"+
//              "'"+p.getInt(10)+"',"+"'"+p.getInt(11)+"',"+"'"+p.getString(12)+"',"+"'"+p.getString(13)+"',"+"'"+p.getString(14)+"'"+",'')";
//
//            stmt.execute(sql);
//            //println(sql)
//            if (i%100 ==0) {
//              stmt.executeBatch()
//              println(i)
//            }
//            i+=1
//        }
//    }
    sc.stop()
  }
}




