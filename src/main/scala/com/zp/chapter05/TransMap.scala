package com.zp.chapter05

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object TransMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .fromElements(
        Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L)
      )
    //1.使用匿名函数的方式提取user段
    stream.map(_.user).print("1")
    //2.使用调用外部类的方式提取user字段
    stream.map(new UserExtractor).print("2")
    env.execute()
  }
  class UserExtractor extends MapFunction[Event,String]{
    override def map(t: Event): String = t.user
  }

}
