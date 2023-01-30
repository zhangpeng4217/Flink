package com.zp.chapter05

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.environment._

object TransFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .fromElements(
        Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L)
      )
//    lambad表达式
    stream.filter(log => "Mary".equals(log.user)).print("1")
//    通过匿名函数类
    stream.filter(new UserFilter).print("2")
    env.execute()

  }
//  自定义FilterFunction函数类
  class UserFilter extends FilterFunction[Event]{
    //是否包含Bob包含就输出，条件成立就输出条件不成立就过滤掉
    override def filter(t: Event): Boolean = "Bob".equals(t.user)
  }

}
