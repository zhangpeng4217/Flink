package com.zp.chapter05

import org.apache.flink.streaming.api.scala._

//定义一个数据类型的样例类
case class Event(user:String,url:String,timestamp: Long)

object SourceBoundedTest {
  def main(args: Array[String]): Unit = {
    //创建Flink的运行环境
      val env = StreamExecutionEnvironment.getExecutionEnvironment
    //从元素中读取数据
    val stream: DataStream[Int] = env.fromElements(1, 2, 3, 5, 6)
    val stream1 = env.fromElements(Event("zp", "./home", 100001L), Event("czx", "./car", 10002L))
    //从集合中读取数据
    val clicks = List(Event("zp", "./home", 100001L), Event("czx", "./car", 10002L))
    val stream2 = env.fromCollection(clicks)
    //打印输出
    stream.print("number")
    stream1.print("1")
    stream2.print("2")
    //使程序保持运行状态
    env.execute()
  }
}
