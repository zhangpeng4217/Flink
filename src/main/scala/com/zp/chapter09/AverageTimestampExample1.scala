package com.zp.chapter09

import com.zp.chapter05.{ClickSource, Event}
import org.apache.flink.api.common.functions.{AggregateFunction, FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{AggregatingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.shell.Count

/**
 * @Author zhangpeng
 * @Date 2023 01 13 13 56
 **/
class AverageTimestampExample1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .flatMap(new AvgTimestamp)
      .print()

    env.execute()
  }
  //实现自定义的RichFlatMapFunction
  class AvgTimestamp extends RichFlatMapFunction[Event,String] {
    //定义一个聚合状态,存放聚合结果
    lazy val avgTsAggState = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[Event,(Long,Long),Long](
      "avg-ts",
      new AggregateFunction[Event,(Long,Long),Long] {
        override def createAccumulator(): (Long, Long) = (0L,0L)

        override def add(in: Event, acc: (Long, Long)): (Long, Long) = (acc._1 + in.timestamp,acc._2 +1L)

        override def getResult(acc: (Long, Long)): Long = acc._1 / acc._2

        override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = ???
      },
      classOf[(Long,Long)]
    ))

    //再定义一个值状态，因为通过聚合状态无法获取数据的个数，所以再定义一个值状态来获取数据个数
    lazy val valueState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state",classOf[Long]))

    override def flatMap(in: Event, collector: Collector[String]): Unit = {
      avgTsAggState.add(in)
      //更新count值
      val count = valueState.value()
      valueState.update(count+1L)

      //判断是否达到计数窗口长度，输出结果
      if (valueState.value() == 5){
        collector.collect(s"用户：${in.user}的平均时间戳为：${avgTsAggState.get()}")

        //窗口销毁
        valueState.clear()
      }

    }
  }
}
