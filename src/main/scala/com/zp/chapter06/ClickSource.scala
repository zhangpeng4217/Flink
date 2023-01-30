package com.zp.chapter06

import java.util.Calendar

import com.zp.chapter05_sink.Event
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

import scala.util.Random

//自定义数据源
class ClickSource extends SourceFunction[Event]{
//  标志位
  var running = true
  override def run(sourceContext: SourceFunction.SourceContext[Event]): Unit = {
//  随机数生成器
    val random = new Random()
//   定义数据随机选择范围
    val users = Array("zp","czx","lhr","bgd","sx","hxb")
    val urls = Array("./home","./cart","./fav","xc","ph","wzxq?id=123456")

//    用标志位作为循环判断条件，不停发出数据
    while (running){
      val event = Event(users(random.nextInt(users.length)),urls(random.nextInt(urls.length)),Calendar.getInstance().getTimeInMillis)
//      //为要发送的数据分配时间戳
//      sourceContext.collectWithTimestamp(event,event.timestamp)
//      直接向下游发送水位线,这样下游就不用定义水位线了
//      sourceContext.emitWatermark(new Watermark(event.timestamp - 1L))
      //使用sourceContext方法向下游发送数据
      sourceContext.collect(event)
      //每隔1s发送一条数据
      Thread.sleep(1000)
    }
    }

  override def cancel(): Unit = running = false
}
