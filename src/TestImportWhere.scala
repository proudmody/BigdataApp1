/**
  * 实验jdbcWhere子句
  */
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.triman.bigdata.util.HDFSUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Logger,Level}

import scala.collection.mutable.ListBuffer

object TEST{
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val Hconf: Configuration = new Configuration
    HDFSUtil.UploadLocalFileHDFS(Hconf, "E:\\rk_spark\\infoGet\\out\\artifacts\\infoGet_jar\\infoGet.jar", "/jar/infoGet.jar")
    val jar = ListBuffer[String]()
    jar += "hdfs://linux90:9000/jar/infoGet.jar"
    jar += "hdfs://linux90:9000/jar/ojdbc6.jar"
    val conf = new SparkConf().setAppName("EXTRACTDATA").setMaster("spark://linux90:7077").
      set("spark.driver.memory", "2g").
      set("spark.executor.memory", "8g").
      set("spark.testing.memory","2147480000")
    conf.setJars(jar)
    val sc = new SparkContext(conf)
    sc.addJar("hdfs://linux90:9000/jar/ojdbc6.jar")
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    val rkDB = "jdbc:oracle:thin:@10.15.58.19:1521:syrk1"
    val rkUser = "syrk"
    val rkPassword = "smc@23977"
    val connectionProperties = new Properties()
    connectionProperties.put("user",rkUser)
    connectionProperties.put("password",rkPassword)
    connectionProperties.put("driver","oracle.jdbc.driver.OracleDriver")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE,-14)
    val d_14 = calendar.getTime
    val d_now = new Date()
    val d4mat = new SimpleDateFormat("yyyyMMdd")
    val lowerbound =  d4mat.format(d_14)+"000000"
    val upbound = d4mat.format(d_now)+"000000"
    val df = sqlContext.read.jdbc(rkDB,"T_WB_TRACE",Array("LOGIN_AT BETWEEN '"+lowerbound+"' AND '"+ upbound +"'"),connectionProperties)
    val base = "hdfs://linux90:9000/user/hadoop/20160825parquet/"
    df.write.parquet(base+"T_WB_TRACE")
    val cnt =df.count()
    println(cnt)
    df.printSchema()
    sc.stop()
  }
}




