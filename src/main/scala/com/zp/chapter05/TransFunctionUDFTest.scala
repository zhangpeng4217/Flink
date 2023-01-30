package com.zp.chapter05

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._
object TransFunctionUDFTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val clicks = List(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L))
    val stream = env.fromCollection(clicks)

    //测试Udf的用法，筛选Url中包含某个关键字ph的Event事件

    //1.实现一个自定义的函数类
    stream.filter(new MyFliterFunction).print("1")

    //执行
    env.execute()

  }
  //实现自定义的filterFunction
  class MyFliterFunction() extends FilterFunction[Event] {
    override def filter(t: Event): Boolean = t.url.contains("ph")
  }
}
