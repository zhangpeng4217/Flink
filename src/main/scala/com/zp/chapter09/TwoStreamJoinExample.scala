package com.zp.chapter09

import com.zp.chapter05.{ClickSource, Event}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TwoStreamJoinExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1 = env
      .fromElements(
        ("a", "stream-1", 1000L),
        ("b", "stream-1", 2000L)
      )
      .assignAscendingTimestamps(_._3)
    val stream2 = env
      .fromElements(
        ("b", "stream-2", 3000L),
        ("a", "stream-2", 4000L)
      )
      .assignAscendingTimestamps(_._3)

    //连接两条流，进行join操作
    stream1.keyBy(_._1)
      .connect(stream2.keyBy(_._1))
      .process(new TwoStreamJoin)
      .print()

    env.execute()
  }
  //实现自定义的CoProcessFunction
  class TwoStreamJoin extends CoProcessFunction[(String,String,Long),(String,String,Long),String]{
    //定义列表状态保存流中已经达到的数据
    lazy val stream1ListState:ListState[(String,String,Long)] = getRuntimeContext.getListState(new ListStateDescriptor[(String, String, Long)]("stream1-list",classOf[(String,String,Long)]))

    lazy val stream2ListState:ListState[(String,String,Long)] = getRuntimeContext.getListState(new ListStateDescriptor[(String, String, Long)]("stream2-list",classOf[(String,String,Long)]))

    override def processElement1(in1: (String, String, Long), context: CoProcessFunction[(String, String, Long), (String, String, Long), String]#Context, collector: Collector[String]): Unit = {
      //将来的数据直接添加到列表状态中
      stream1ListState.add(in1)
      //遍历另一条流中已经到达的所有数据，输出配对信息
      import scala.collection.convert.ImplicitConversions._  //隐式转换使用
      for (value2 <- stream2ListState.get){
        collector.collect(in1 + "=>" + value2)
      }
    }

    override def processElement2(in2: (String, String, Long), context: CoProcessFunction[(String, String, Long), (String, String, Long), String]#Context, collector: Collector[String]): Unit = {
      //将来的数据直接添加到列表状态中
      stream2ListState.add(in2)
      //遍历另一条流中已经到达的所有数据，输出配对信息
      import scala.collection.convert.ImplicitConversions._  //隐式转换使用
      for (value1 <- stream1ListState.get){
        collector.collect(value1 + "=>" + in2)
      }
    }
  }
}
