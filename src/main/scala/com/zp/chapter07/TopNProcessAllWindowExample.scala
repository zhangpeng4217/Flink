package com.zp.chapter07

import com.zp.chapter06.ClickSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

object TopNProcessAllWindowExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    //直接开窗统计
    stream.map(_.url)
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .process(new ProcessAllWindowFunction[String,String,TimeWindow] {
        override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
          //1.统计每个Url的访问次数
          //初始化一个map(url,count)  map默认是不可变的，我们需要使用可变的map所以要用mutable
          val urlCountMap = mutable.Map[String,Long]()
          //循环将传来的数据插入到map集合中
          elements.foreach(
            data =>
              urlCountMap.get(data) match {
              case Some(count) => urlCountMap.put(data,count+1L) //option的匹配模式：如果count有值则需要加一
              case None => urlCountMap.put(data,1L)//如果count没有值则，复制为一
            }
          )
          //2.先将map转换为列表，然后对其进行排序,然后倒序，也可以直接在_._2前面加一个负号
          val urlCountList = urlCountMap.toList.sortBy(_._2).reverse.take(2)
          //3.包装信息打印输出
          val result = new mutable.StringBuilder()
          result.append(s"=======窗口：${context.window.getStart} ~ ${context.window.getEnd}========\n")
          for (i <- urlCountList.indices) {
            val tuple = urlCountList(i)
            result.append(s"浏览量Top ${i+1}  ")
            result.append(s"url: ${tuple._1}")
            result.append(s"浏览量：${tuple._2}\n")
          }
          out.collect(result.toString())
        }
      }).print()
    env.execute()
  }
}
