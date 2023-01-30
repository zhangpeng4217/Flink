package com.zp.chapter08

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowJoinTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //方便测试

    val stream1 = env.fromElements(
      ("a", 1000L),
      ("b", 1000L),
      ("a", 2000L),
      ("b", 2000L)
    ).assignAscendingTimestamps(_._2)
    val stream2 = env.fromElements(
      ("a", 3000L),
      ("b", 3000L),
      ("a", 4000L),
      ("b", 4000L)
    ).assignAscendingTimestamps(_._2)

    //窗口联结操作
    stream1.join(stream2)
      .where(_._1) //指定第一条流中的元素key
      .equalTo(_._1) //指定第二条流中元素的key
      .window(TumblingEventTimeWindows.of(Time.seconds(5))) //开窗口
      .apply(new JoinFunction[(String,Long),(String,Long),String] {
        override def join(in1: (String, Long), in2: (String, Long)): String = {
          in1 + "=>" + in2
        }
      }).print()
    env.execute()
  }
}
