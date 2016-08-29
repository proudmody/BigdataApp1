package com.triman.bigdata.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

/**
  * 初始化SparkApp
  */
object Initial_Spark{
  def Initial_Spark(appName:String,tjsj:String)= {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val Hconf: Configuration = new Configuration
    HDFSUtil.UploadLocalFileHDFS(Hconf, "E:\\rk_spark\\infoGet\\out\\artifacts\\infoGet_jar\\infoGet.jar", "/jar/infoGet.jar")
    val jar = ListBuffer[String]()
    jar += "hdfs://linux90:9000/jar/infoGet.jar"
    jar+="hdfs://linux90:9000/jar/ojdbc6.jar"
    val conf = new SparkConf().setAppName(tjsj+"-"+appName).setMaster("spark://linux90:7077").
      set("spark.driver.memory", "2g").
      set("spark.executor.memory", "12g").
      set("spark.testing.memory","2147480000")
    conf.setJars(jar)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    sqlContext.setConf("spark.sql.shuffle.partitions","12")
    (sc,sqlContext)
  }
}
