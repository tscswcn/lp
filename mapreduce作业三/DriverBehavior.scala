package com.huawei.bigdata.spark.examples

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.{SparkConf, SparkContext}

object DriverBehavior {
  // 表结构,后面用来将文本数据映射为DataFrame 
  case class DriverInfo(
  driverID :String,
  carNumber :String,
  latitude :String,
  longitude :String,
  speed :Int,
  direction :Int,
  siteName :String,
  time :String,
  isRapidlySpeedup :Int,
  isRapidlySlowdown :Int,
  isNeutralSlide :Int,
  isNeutralSlideFinished :Int,
  neutralSlideTime :Int,
  isOverspeed :Int,
  isOverspeedFinished :Int,
  overspeedTime :Int,
  isFatigueDriving :Int,
  isHthrottleStop :Int,
  isOilLeak :Int)

  def main(args: Array[String]) {
    // 定义Spark执行的sql,查询车主的各种违法情况的总次数和时间
    var sql =
      """select
         driverID,
         carNumber,
         sum(isRapidlySpeedup) as rapidlySpeedupTimes,
         sum(isRapidlySlowdown) as rapidlySlowdownTimes,
         sum(isNeutralSlide) as neutralSlideTimes,
         sum(neutralSlideTime) as neutralSlideTimeTotal,
         sum(isOverspeed) as overspeedTimes,
         sum(overspeedTime) as overspeedTimeTotal,
         sum(isFatigueDriving) as fatigueDrivingTimes,
         sum(isHthrottleStop) as hthrottleStopTimes,
         sum(isOilLeak) as oilLeakTimes
        from
         driver_behavior
        where
         time >= "2017-01-01 00:00:00"
         and time <= "2017-02-01 00:00:00"
         and (isRapidlySpeedup > 0
        OR isRapidlySlowdown > 0
        OR isNeutralSlide > 0
        OR isNeutralSlideFinished > 0
        OR isOverspeed > 0
        OR isOverspeedFinished > 0
        OR isFatigueDriving > 0
        OR isHthrottleStop > 0
        OR isOilLeak > 0)
        group by
         driverID,
         carNumber
        order by
         rapidlySpeedupTimes desc,
         rapidlySlowdownTimes desc,
         neutralSlideTimes desc,
         neutralSlideTimeTotal desc,
         overspeedTimes desc,
         overspeedTimeTotal desc,
         fatigueDrivingTimes desc,
         hthrottleStopTimes desc,
         oilLeakTimes desc
      """
    
    // 从传入的参数中获取ak、sk、任务数、数据输入目录和输出目录
    var ak = args(0)
    var sk = args(1)

    var input = ""
    var output = ""
    var taskCount  = -1;
    if (args.length == 4) {
      input = args(2)
      output = args(3)
    } else if (args.length == 5) {
      taskCount = args(2).trim.toInt
      input = args(3)
      output = args(4)
    }

    println("Start spark task with args: %s", args.mkString(","))
    
    // 配置Spark应用名称
    val sparkConf = new SparkConf().setAppName("DriverBehavior")
    sparkConf.set("spark.hadoop.fs.s3a.access.key", ak)
    sparkConf.set("spark.hadoop.fs.s3a.secret.key", sk)

    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // 对录入数据按照“,”进行分割,然后转换成DataFrame并注册表
    sc.textFile(input).map(_.split(","))
      .map(p => {
        val driverID :String = getStringOrEmpty(p, 0)
        val carNumber :String = getStringOrEmpty(p, 1)
        val latitude :String = getStringOrEmpty(p, 2)
        val longitude :String = getStringOrEmpty(p, 3)
        val speed :Int = getIntOrEmpty(p, 4)
        val direction :Int = getIntOrEmpty(p, 5)
        val siteName :String = getStringOrEmpty(p, 6)
        val time :String = getStringOrEmpty(p, 7)
        val isRapidlySpeedup :Int = getIntOrEmpty(p, 8)
        val isRapidlySlowdown :Int = getIntOrEmpty(p, 9)
        val isNeutralSlide :Int = getIntOrEmpty(p, 10)
        val isNeutralSlideFinished :Int = getIntOrEmpty(p, 11)
        val neutralSlideTime :Int = getIntOrEmpty(p, 12)
        val isOverspeed :Int = getIntOrEmpty(p, 13)
        val isOverspeedFinished :Int = getIntOrEmpty(p, 14)
        val overspeedTime :Int = getIntOrEmpty(p, 15)
        val isFatigueDriving :Int = getIntOrEmpty(p, 16)
        val isHthrottleStop :Int = getIntOrEmpty(p, 17)
        val isOilLeak :Int = getIntOrEmpty(p, 18)
        DriverInfo(driverID,
          carNumber,
          latitude,
          longitude,
          speed,
          direction,
          siteName,
          time,
          isRapidlySpeedup,
          isRapidlySlowdown,
          isNeutralSlide,
          isNeutralSlideFinished,
          neutralSlideTime,
          isOverspeed,
          isOverspeedFinished,
          overspeedTime,
          isFatigueDriving,
          isHthrottleStop,
          isOilLeak)
      })
      .toDF.registerTempTable("driver_behavior")

    // 执行sql
    val data = sqlContext.sql(sql)

    // 输出结果数据到输出目录
    if (taskCount != -1) {
      data.repartition(taskCount).write.format("csv").save(output)
    } else {
      data.write.format("csv").save(output)
    }
    sc.stop()
  }
  
  // 对数据中string类型的数据进行处理
  def getStringOrEmpty(data: Array[String], index: Int) : String = {
    if (data.length > index)
      data(index)
    else ""
  }

  // 对数据中int类型的数据进行处理
  def getIntOrEmpty(data: Array[String], index: Int) : Int = {
    if (data.length > index && data(index) != "")
      data(index).trim.toInt
    else 0
  }
}
