package com.triman.bigdata.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

class JdbcConn{

  def getZW_wtsj(tjsj:String):ArrayBuffer[record]={
    var  con:Connection= null;// 创建一个数据库连接
    var  pre:PreparedStatement = null;// 创建预编译语句对象，一般都是用这个而不用Statement
    var  result:ResultSet= null;// 创建一个结果集对象
    try
    {
      Class.forName("oracle.jdbc.driver.OracleDriver");// 加载Oracle驱动程序
      val url = "jdbc:oracle:thin:@10.15.58.19:1521:syrk1";// 127.0.0.1是本机地址，XE是精简版Oracle的默认数据库名
      val user = "syrk";// 用户名,系统默认的账户名
      val password = "smc@23977";// 你安装时选设置的密码
      con = DriverManager.getConnection(url, user, password);// 获取连接
      val sql = "select fwbm,wzjz,zzjz from T_FW_WTSJ t where tjsj ='"+tjsj+"'";

      val stmt = con.createStatement()

      result = stmt.executeQuery(sql);// 执行查询，注意括号中不需要再加参数
      val list:ArrayBuffer[record]=new ArrayBuffer[record]
      while (result.next()){
        // 当结果集不为空时
        val tmp =record(result.getString("FWBM"),result.getInt("WZJZ"), result.getInt("ZZJZ"))
        list.append(tmp)
       // System.out.println(result.getString("HPHM"))
      }
      list
    }
    catch {
      case e:Exception => e.printStackTrace(); null
    }
    finally
    {
      try
      {
        // 逐一将上面的几个对象关闭，因为不关闭的话会影响性能、并且占用资源
        // 注意关闭的顺序，最后使用的最先关闭
        if (result != null)
          result.close();
        if (pre != null)
          pre.close();
        if (con != null)
          con.close();
        System.out.println("数据库连接已关闭！");
      }
      catch {
        case e:Exception => e.printStackTrace();
      }
    }
  }
  def Inser(df:DataFrame):ArrayBuffer[record]={
    var  con:Connection= null;// 创建一个数据库连接
    var  pre:PreparedStatement = null;// 创建预编译语句对象，一般都是用这个而不用Statement
    var  result:ResultSet= null;// 创建一个结果集对象
    try
    {
      Class.forName("oracle.jdbc.driver.OracleDriver");// 加载Oracle驱动程序
      System.out.println("开始尝试连接数据库！")
      val url = "jdbc:oracle:thin:@10.15.58.19:1521:syrk1";// 127.0.0.1是本机地址，XE是精简版Oracle的默认数据库名
      val user = "syrk";// 用户名,系统默认的账户名
      val password = "smc@23977";// 你安装时选设置的密码
      con = DriverManager.getConnection(url, user, password);// 获取连接
      System.out.println("连接成功！")
      val sql = "inser";

      val stmt = con.createStatement()

      result = stmt.executeQuery(sql);// 执行查询，注意括号中不需要再加参数
      val list:ArrayBuffer[record]=new ArrayBuffer[record]
      while (result.next()){
        // 当结果集不为空时
        val tmp =record(result.getString("FWBM"),result.getInt("WZJZ"), result.getInt("ZZJZ"))
        list.append(tmp)
        // System.out.println(result.getString("HPHM"))
      }
      list
    }
    catch {
      case e:Exception => e.printStackTrace(); null
    }
    finally
    {
      try
      {
        // 逐一将上面的几个对象关闭，因为不关闭的话会影响性能、并且占用资源
        // 注意关闭的顺序，最后使用的最先关闭
        if (result != null)
          result.close();
        if (pre != null)
          pre.close();
        if (con != null)
          con.close();
        System.out.println("数据库连接已关闭！");
      }
      catch {
        case e:Exception => e.printStackTrace();
      }
    }
  }
}