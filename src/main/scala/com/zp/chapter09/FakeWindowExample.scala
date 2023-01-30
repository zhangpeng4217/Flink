package com.zp.chapter09

import com.zp.chapter05.{ClickSource, Event}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FakeWindowExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp) //注册水位线
      .keyBy(_.url)
      .process(new FakeWindow(10*1000L)) //模拟一个10s的滚动窗口
      .print()

    env.execute()
  }
  //实现自定义的FakeWindow
  class FakeWindow(size:Long) extends KeyedProcessFunction[String,Event,String] {
    //定义map状态  用来保存每个窗户的pv值（模拟窗口所以key的类型还是Long）
    lazy val windowPvMapState = getRuntimeContext.getMapState[Long,Long](new MapStateDescriptor[Long,Long]("map",classOf[Long],classOf[Long]))

    override def processElement(i: Event, context: KeyedProcessFunction[String, Event, String]#Context, collector: Collector[String]): Unit = {
      //计算当前数据落入的窗口起始和结束时间戳
      val start = (i.timestamp/size)*size
      val end = start+size
      //注册一个定时器，用来触发窗口计算
      context.timerService().registerEventTimeTimer(end-1)

      //更新状态
      if(windowPvMapState.contains(start)){
        val pv = windowPvMapState.get(start)
        windowPvMapState.put(start,pv + 1L)
      } else {
        windowPvMapState.put(start,1L)
      }
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
      //定时器触发时，窗口输出结果
      val start = timestamp + 1 - size
      //根据窗口的起始值，获取相应的pv
      val pv = windowPvMapState.get(start)
      //根据ctx获取当前的url,通过out输出
      out.collect(s"url:${ctx.getCurrentKey} 浏览量为：${pv} 窗口为：${start} ~ ${timestamp+1L}")
      out.collect((start+size).toString)
      //窗口销毁
      windowPvMapState.remove(start)
    }
  }

}
