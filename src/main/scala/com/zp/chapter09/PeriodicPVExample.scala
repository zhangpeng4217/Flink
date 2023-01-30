package com.zp.chapter09

import com.zp.chapter05.{ClickSource, Event}
import com.zp.chapter09.KeyedStateTest.Myflatmap
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object PeriodicPVExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .process(new PeriodicPV)
      .print()

    env.execute()
  }
  //实现自定义的keyedProcessFunction
  class PeriodicPV extends KeyedProcessFunction[String,Event,String] {

    //定义值状态，保存当前用户的pv数据
    lazy val countState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count",classOf[Long]))
    //定义值状态，保存注册的定时器时间戳
    lazy val timerTsState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts",classOf[Long]))

    override def processElement(i: Event, context: KeyedProcessFunction[String, Event, String]#Context, collector: Collector[String]): Unit = {
      //每来一个数据就向状态中count加1,因为前面咱们通过key进行分区了所以 不同的key会去寻找不同的状态
      val count = countState.value()
      countState.update(count + 1)

      //注册定时器，每隔10s输出一次统计结果
      if (timerTsState.value() == 0L){
        context.timerService().registerEventTimeTimer(i.timestamp + 10 * 1000L)
        //更新状态
        timerTsState.update(i.timestamp + 10 * 1000L)
      }
    }
    //定时器触发，输出当前的统计结果
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(s"用户${ctx.getCurrentKey}的pv值为：${countState.value()}")
      //清理状态
      timerTsState.clear()
    }
  }
}
