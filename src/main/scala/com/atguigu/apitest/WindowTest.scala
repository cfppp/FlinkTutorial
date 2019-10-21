package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2019/10/21 10:19
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(300L)

    //    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      //      .assignAscendingTimestamps(_.timestamp * 1000L)
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(2500)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
    } )
//      .assignTimestampsAndWatermarks( new MyAssigner() )

    // 每个传感器每隔15秒输出这段时间内的最小值
    val minTempPerWindowStream = dataStream
      .keyBy(_.id)
      .timeWindow(Time.seconds(15))
      .minBy("temperature")
    //      .reduce( (x, y) => SensorReading( x.id, y.timestamp, x.temperature.min(y.temperature) ) )

    dataStream.print("data")
    minTempPerWindowStream.print("min")

    env.execute("window test")
  }
}

class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
  // 定义一个最大延迟时间
  val bound: Long = 1000L
  // 定义当前最大的时间戳
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000)
    element.timestamp * 1000L
  }
}

class MyAssigner2 extends AssignerWithPunctuatedWatermarks[SensorReading]{
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if(lastElement.id == "sensor_1"){
      new Watermark(extractedTimestamp)
    } else{
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}