package com.zp.chapter07

import com.zp.chapter06.{ClickSource, UrlViewCount}
import com.zp.chapter06.UvViewCountExample.{UrlViewCountAgg, UrlviewCountResult}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable

object TopNKeyedProcessFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //生成相应的水位线
    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp) //有序流的水位线生成策略

    //1.结合使用增量聚合函数和全窗口函数，统计每个url的访问次数
    val urlCountStream = stream.keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .aggregate(new UrlViewCountAgg, new UrlviewCountResult)
    //2.按照窗口信息进行分组提取，排序输出
    val resultStream = urlCountStream.keyBy(_.windowEnd)
      .process(new TopN(2))

    //3.打印输出
    resultStream.print()
    env.execute()
  }

  //实现自定义的keyedProcessFunction
  class TopN(n: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

    //声明列表状态
    var urlViewCountListState: ListState[UrlViewCount] = _

    // 定义列表状态，存储 UrlViewCount 数据
    override def open(parameters: Configuration): Unit = {
      urlViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("list-state", classOf[UrlViewCount]))
    }
    //每来一条数据，都会经过此方法
    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
      //每来一个数据都放入liststate中
      urlViewCountListState.add(i)
      //注册一个窗口结束时间1ms之后的定时器
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 导入隐式类型转换,为了下面list的转换
      import scala.collection.JavaConversions._
      //先把数据提取出来放到list里
      val urlViewCount = urlViewCountListState.get().toList
      // 由于数据已经放入 List 中，所以可以将状态变量手动清空了
      urlViewCountListState.clear()
      val topnList = urlViewCount.sortBy(-_.count).take(n)
      //拼接要输出的字符串
      val result = new mutable.StringBuilder()
      result.append(s"=======窗口：${timestamp - 1 - 10000} ~ ${timestamp - 1}========\n")
      for (i <- topnList.indices) {
        val urlViewCount = topnList(i)
        result.append(s"浏览量Top ${i + 1}  ")
        result.append(s"url: ${urlViewCount.url}")
        result.append(s"浏览量：${urlViewCount.count}\n")
      }
      out.collect(result.toString())
    }
  }
}
