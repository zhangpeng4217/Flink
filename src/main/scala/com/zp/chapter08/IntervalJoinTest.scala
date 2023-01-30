package com.zp.chapter08

import com.zp.chapter05.Event
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 订单事件流
    val orderStream: DataStream[(String, String, Long)] = env
      .fromElements(
        ("Mary", "order-1", 5000L),
        ("Alice", "order-2", 5000L),
        ("Bob", "order-3", 20000L),
        ("Alice", "order-4", 20000L),
        ("Cary", "order-5", 51000L)
      ).assignAscendingTimestamps(_._3)

    // 点击事件流
    val pvStream: DataStream[Event] = env
      .fromElements(
        Event("Bob", "./cart", 2000L),
        Event("Alice", "./prod?id=100", 3000L),
        Event("Alice", "./prod?id=200", 3500L),
        Event("Bob", "./prod?id=2", 2500L),
        Event("Alice", "./prod?id=300", 36000L),
        Event("Bob", "./home", 30000L),
        Event("Bob", "./prod?id=1", 23000L),
        Event("Bob", "./prod?id=3", 33000L)
      ).assignAscendingTimestamps(_.timestamp)

    //两条流进行间隔联结，输出匹配结果
    orderStream.keyBy(_._1) //第一条数据流的key
      .intervalJoin(pvStream.keyBy(_.user)) //要join的数据流要匹配的key
      .between(Time.seconds(-5),Time.seconds(10))//指定间隔
      .process(new ProcessJoinFunction[(String,String,Long),Event,String] {
        override def processElement(in1: (String, String, Long), in2: Event, context: ProcessJoinFunction[(String, String, Long), Event, String]#Context, collector: Collector[String]): Unit = {
            collector.collect(in1 + "=>" + in2)
        }
      }).print()
    env.execute()
  }
}
