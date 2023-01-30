package com.zp.chapter06

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
//定义一个数据类型的样例类

object WatermarkTest01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream:DataStream[Event] = env.socketTextStream("master",7777)
      .map(data => {
        val fields = data.split(",")
        Event(fields(0).trim,fields(1).trim,fields(2).trim.toLong)
      })

//    1.有序流的水位线生成策略
val value = stream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[Event]()
  .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
    override def extractTimestamp(t: Event, l: Long): Long = t.timestamp //确定好水位线的时间戳
  })
)

//    2.乱序流的水位线生成方法
    stream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(2))  //这里多了一个时间范围
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
        })
    )
        .keyBy(_.user)
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .process(new WatermarkWindowResult)
        .print()



    //生产水位线通用方法，自由但麻烦
    stream.assignTimestampsAndWatermarks(new WatermarkStrategy[Event] {
      // //确定好那个字段作为水位线的时间戳
      override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Event] = {
        new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
        }
      }

      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Event] = {
        new WatermarkGenerator[Event] {
          //定义一个延迟时间
          val delay = 5000L
          //定义属性保存的最大时间戳
          var maxTs = Long.MinValue+delay+1

          //每个事件（数据）到来都会调用的方法，它的参数有当前事件、时间戳，
          //以及允许发出水位线的一个 WatermarkOutput
          override def onEvent(t: Event, l: Long, watermarkOutput: WatermarkOutput): Unit ={
            maxTs = math.max(maxTs,t.timestamp)
          }
          //周期性调用的方法，可以由 WatermarkOutput 发出水位线。周期时间
          //为处理时间，可以调用环境配置的 setAutoWatermarkInterval()方法来设置，默认为
          //200ms。
          override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
            val watermark = new Watermark(maxTs - delay -1)
            watermarkOutput.emitWatermark(watermark)
          }
        }
      }
    })

 env.execute()
  }
  //实现自定义的全窗口函数
  class WatermarkWindowResult extends ProcessWindowFunction[Event,String,String,TimeWindow] {
    override def process(user: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
      //提取信息
      val start = context.window.getStart
      val end = context.window.getEnd
      val count = elements.size
      //增加水位线信息
      val currentWatermark = context.currentWatermark

      out.collect(s"窗口 $start ~ $end , 用户 $user 的活跃度为： $count,水位线位于：$currentWatermark")
    }

  }
}
