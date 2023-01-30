package com.zp.chapter07

import com.zp.chapter06.{ClickSource, Event}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //为了方便测试与输出
    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
    stream.process(new ProcessFunction[Event,String] {
      //每来一个数据都会调用一次
      override def processElement(i: Event, context: ProcessFunction[Event, String]#Context, collector: Collector[String]): Unit = {
        if ("czx".equals(i.user)){
          //向下游发送数据
          collector.collect(i.user)
        }else if(i.user.equals("zp")){
          collector.collect(i.user)
          collector.collect(i.url)
        }

        //打印当前水位线
        println(context.timerService().currentWatermark())
      }
    }).print()

    env.execute()
  }
}
