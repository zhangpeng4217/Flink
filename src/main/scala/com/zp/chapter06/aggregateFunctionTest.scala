package com.zp.chapter06

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object aggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //生成相应的水位线
    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp) //有序流的水位线生成策略

    //统计Pv 和 Uv  输出 pv/uv
    stream.keyBy(data => true)
      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))
      .aggregate(new PvUv)
      .print()

    env.execute()

  }
  //实现自定义的聚合函数，用一个二元组（long,set）来表示中间聚合的（pv,uv）状态  这里的set用来去重
  class PvUv extends AggregateFunction[Event,(Long,Set[String]),Double]{
    //acc的初始化
    override def createAccumulator(): (Long, Set[String]) = (0L,Set[String]())
    //每来一条数据，都会进行add叠加聚合
    override def add(in: Event, acc: (Long, Set[String])): (Long, Set[String]) = (acc._1+1,acc._2+in.user)
    //返回最终计算结果
    override def getResult(acc: (Long, Set[String])): Double = acc._1.toDouble/acc._2.size
    //三个？ 作为占位符，这里没有窗口的合并所以不实现这个方法
    override def merge(acc: (Long, Set[String]), acc1: (Long, Set[String])): (Long, Set[String]) = ???
  }
}
