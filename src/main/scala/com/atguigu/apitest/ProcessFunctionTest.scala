package com.atguigu.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2019/10/21 16:21
  */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
    //      .assignTimestampsAndWatermarks( new MyAssigner() )

    // 温度连续上升报警
    val warningStream = dataStream
      .keyBy(_.id)
      .process( new TempIncreseWarning() )

    // 低温冰点报警
    val freezingMonitorStream = dataStream
      .process( new FreezingMonitor() )

    dataStream.print("data")
    freezingMonitorStream.print("healthy")
    freezingMonitorStream.getSideOutput(new OutputTag[(String, String)]("freezing-warning")).print("freezing")

    env.execute("process function test")
  }
}

class TempIncreseWarning() extends KeyedProcessFunction[String, SensorReading, String]{

  // 定义一个状态，用户保存上一次的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("lastTemp-state", Types.of[Double]) )
  // 定义一个状态，用户保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("currentTimer-state", Types.of[Long]) )

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("sensor " + ctx.getCurrentKey + "温度10秒内连续上升")
    currentTimer.clear()
  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 取出上一次的温度值
    val prevTemp = lastTemp.value()
    lastTemp.update(value.temperature)

    val curTimerTs = currentTimer.value()

    // 如果温度上升，而且没有设置过定时器，就注册一个定时器
    if( value.temperature > prevTemp && curTimerTs == 0 ){
      val timerTs = ctx.timerService().currentProcessingTime() + 10000L
      ctx.timerService().registerProcessingTimeTimer( timerTs )
      // 保存时间戳到状态
      currentTimer.update(timerTs)
    }
    // 如果温度下降的话，删除定时器
    else if( value.temperature < prevTemp ){
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    }
  }
}

class FreezingMonitor() extends ProcessFunction[SensorReading, (String, Double, String)]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, (String, Double, String)]#Context, out: Collector[(String, Double, String)]): Unit = {
    if( value.temperature < 32.0 ){
      ctx.output( new OutputTag[(String, String)]("freezing-warning"), (value.id, "freezing") )
    } else {
      out.collect( (value.id, value.temperature, "healthy") )
    }
  }
}