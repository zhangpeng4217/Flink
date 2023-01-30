package com.zp.chapter05

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object TransFlatmap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("zp", "./cart", 2000L)
    )
    stream.flatMap(new MyFlatMap).print()
    env.execute()
  }
  class MyFlatMap extends FlatMapFunction[Event,String]{
    override def flatMap(t: Event, collector: Collector[String]): Unit = {
      //如果是 Mary 的点击事件，则向下游发送 1 次，如果是 Bob 的点击事件，则向下游发送 2 次,不符合条件的就会过滤掉
      if(t.user.equals("Mary")){
        collector.collect(t.user)
      }else if(t.user.equals("Bob")){
        collector.collect(t.user)
        collector.collect(t.timestamp.toString)
      }
      }
  }
}
