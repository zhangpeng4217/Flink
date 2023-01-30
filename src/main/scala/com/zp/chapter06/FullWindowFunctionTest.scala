package com.zp.chapter06

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FullWindowFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //生成相应的水位线
    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp) //有序流的水位线生成策略

    //测试全窗口函数，统计uv  key => 常量  也就是不分词
    stream.keyBy(data => "key")
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new UvCountByWindow)
      .print()

    env.execute()

  }
  //自定义实现ProcessWindowFunction
  class UvCountByWindow extends ProcessWindowFunction[Event,String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
      //使用一个Set集合实现去重的过程
      var userSet = Set[String]()

      //从element中提取所有数据，依次放入set中去重
      elements.foreach(userSet += _.user)
      val uv = userSet.size
      //提取窗口信息包装String进行输出
      val windowEnd = context.window.getEnd
      val windowStart = context.window.getStart

      //输出结果
      out.collect(s"窗口 $windowStart ~ $windowEnd 的uv值为：$uv")
    }
  }
}
