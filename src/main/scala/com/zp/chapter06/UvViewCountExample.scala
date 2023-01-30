package com.zp.chapter06

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
//定义输出的结果数据结构
case class UrlViewCount(url:String,count:Long,windowStart:Long,windowEnd:Long)

object UvViewCountExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //生成相应的水位线
    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp) //有序流的水位线生成策略

    //结合使用增量聚合函数和全窗口函数，包装统计信息
    stream.keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .aggregate(new UrlViewCountAgg,new UrlviewCountResult)
      .print()
    env.execute()
  }
  //实现聚合函数，每来一个数据就加一(前面已经分好按key进行相关的区分窗口)
  class UrlViewCountAgg extends AggregateFunction[Event,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: Event, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = ???
  }
  //实现全窗口函数
  class UrlviewCountResult extends ProcessWindowFunction[Long,UrlViewCount,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      //提取需要的数据
      val count = elements.iterator.next()
      val start = context.window.getStart
      val end = context.window.getEnd
      //输出数据
      out.collect(UrlViewCount(key,count,start,end))
    }
  }
}
