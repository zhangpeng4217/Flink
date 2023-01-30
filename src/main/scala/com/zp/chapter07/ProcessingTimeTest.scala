package com.zp.chapter07

import com.zp.chapter06.{ClickSource, Event}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessingTimeTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //为了方便测试与输出
    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
    stream.keyBy( data => "true")
      .process(new KeyedProcessFunction[String,Event,String] {
        override def processElement(i: Event, context: KeyedProcessFunction[String, Event, String]#Context, collector: Collector[String]): Unit = {
          val currenTime = context.timerService().currentProcessingTime()
          collector.collect(s"数据到达时间是：$currenTime")
          //注册一个5秒的定时器
          context.timerService().registerProcessingTimeTimer(currenTime+5*1000)
        }
          //定义定时器触发时的执行逻辑
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect("定时器的触发事件是"+timestamp)
        }
      }).print()

    env.execute()
  }

}
