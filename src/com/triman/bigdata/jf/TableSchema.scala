package com.triman.bigdata.jf

/**
  * Created by hadoop on 2016/3/22.
  */
import java.util.Properties

import com.triman.bigdata.util.SavaJdbc
import com.triman.bigdata.util.HDFSUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

object TableSchema{

  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val Hconf: Configuration = new Configuration
    HDFSUtil.UploadLocalFileHDFS(Hconf, "E:\\rk_spark\\infoGet\\out\\artifacts\\infoGet_jar\\infoGet.jar", "/jar/infoGet.jar")
    val jar = ListBuffer[String]()
    jar += "hdfs://linux90:9000/jar/infoGet.jar"
    jar += "hdfs://linux90:9000/jar/ojdbc6.jar"
    val conf = new SparkConf().setAppName("Rksj_test").setMaster("spark://linux90:7077").
      set("spark.driver.memory", "2g").
      set("spark.executor.memory", "8g")
    conf.setJars(jar)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    sqlContext.setConf("spark.sql.shuffle.partitions","10")

    val base = "hdfs://linux90:9000/user/hadoop/20160822parquet/"
    val T_ZDRY_JBXX = sqlContext.read.parquet(base+"T_ZDRY_JBXX_OLD1")
    val JFres = sqlContext.read.parquet("JFres")
    val out = JFres.selectExpr("ZJHM1", "STOTAL1", "SQK1", "STOTAL2", "SQK2",
  "STOTAL3", "SQK3",  "STOTAL4", "SQK4", "STOTAL5","SQK5")

    val User = "priapweb"
    val Password = "priapweb"
    val connectionProperties = new Properties()
    connectionProperties.put("user",User)
    connectionProperties.put("password",Password)
    SavaJdbc.Save(out,"jdbc:oracle:thin:@10.15.61.36:1521:jzpt","T_JF_TMP",connectionProperties)
    sc.stop()
  }
}




