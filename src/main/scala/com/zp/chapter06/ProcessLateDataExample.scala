package com.zp.chapter06

import java.time.Duration

import com.zp.chapter06.UvViewCountExample.{UrlViewCountAgg, UrlviewCountResult}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object ProcessLateDataExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //生成相应的水位线
    val stream:DataStream[Event] = env.socketTextStream("master",7777)
      .map(data => {
        val fields = data.split(",")
        Event(fields(0).trim,fields(1).trim,fields(2).trim.toLong)
      })

    //定义一个输出流标签
    val outputTag = OutputTag[Event]("late-data")
    val result: DataStream[UrlViewCount] = stream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(2)) //这里多了一个时间范围
      .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
      })
    )
      .keyBy(_.url)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      //指定窗口允许等待的时间
      .allowedLateness(Time.minutes(1))
      //将迟到数据输出到侧输出流
      .sideOutputLateData(outputTag)
      .aggregate(new UrlViewCountAgg, new UrlviewCountResult)

    //输出初始数据
    stream.print("input")
    //输出结果数据
    result.print("result")
    //输出侧输出流的数据打印输出
    result.getSideOutput(outputTag).print("late data")

    env.execute()
  }
}
