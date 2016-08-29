import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Timer, TimerTask}

import org.apache.spark.deploy.SparkSubmit
import java.io

import com.triman.bigdata.jf.{ZDRY_JF_DATAEX, mainProcess}
import com.triman.bigdata.syrk.{DataExtract, job, job2, job_16_1}

import scala.math._

object TimerSchedule {
  def main(args: Array[String]) {
    //定时任务调度
    val timer = new Timer()
    timer.schedule(new MyTask(),0,1000*30)
  }
  //每个月一号
  var nextDatePerMonth:Date = null
  //每个月16号
  var nextDatePerMonthHalf:Date = null
  //每天
  var nextDatePerDay:Date = null
  //循环计数
  var loopcnt = 0
  class MyTask() extends TimerTask with Serializable {
    @Override
    def run: Unit = {
        val now = new Date()
        //初始化
        if (nextDatePerMonth==null){
           nextDatePerMonth=setTimePerMonth(1,1,4)
          println(s"SET $nextDatePerMonth as nextDatePerMonth ")
        }
        //超过预定时间就执行
        else if (now.after(nextDatePerMonth)) {
          println("processing")
          DataExtract.main(null)
          job.main(null)
         job2.main(null)
          nextDatePerMonth=setTimePerMonth(1,1,4)
          println(s"SET $nextDatePerMonth as nextDatePerMonth ")
        }
      //初始化
        if (nextDatePerMonthHalf==null){
          val d = new Date()
          val mon = if (d.getDate >15)  1 else 0
          nextDatePerMonthHalf=setTimePerMonth(mon,16,4)
          println(s"SET $nextDatePerMonthHalf as nextDatePerMonthHalf ")
        }
        //超过预定时间就执行
        else if(now.after(nextDatePerMonthHalf)) {
          println("processing")
          DataExtract.main(null)
          job_16_1.main(null)
          nextDatePerMonth=setTimePerMonth(1,16,4)
          println(s"SET $nextDatePerMonthHalf as nextDatePerMonthHalf ")
        }
      //初始化
       if(nextDatePerDay==null){
         nextDatePerDay = setTimePerDay(0)
         println(s"SET $nextDatePerDay as nextDatePerDay")
       }
       //超过预定时间就执行
       else if(now.after(nextDatePerDay)) {
          println("processing")
          ZDRY_JF_DATAEX.main(null)
          mainProcess.main(null)
         println(s"SET $nextDatePerDay as nextDatePerDay")
       }
        if (loopcnt%20==0) {
          println(s"Timer is Runing,loopTime = $loopcnt *30 sec")
        }
      loopcnt+=1
      }
    }
  //每月几号几点执行
  def setTimePerMonth(month:Int,day:Int,hour:Int):Date = {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.MONTH,month)
    calendar.set(Calendar.DAY_OF_MONTH,day)
    calendar.set(Calendar.HOUR_OF_DAY,hour)
    calendar.set(Calendar.MINUTE,0)
    calendar.set(Calendar.SECOND,0)
    val time = calendar.getTime
    time
  }
  //每天几点执行
  def setTimePerDay(hour:Int):Date = {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH,1)
    calendar.set(Calendar.HOUR_OF_DAY,hour)
    calendar.set(Calendar.MINUTE,0)
    calendar.set(Calendar.SECOND,0)
    val time = calendar.getTime
    time
  }
}






