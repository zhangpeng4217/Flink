package com.zp.chapter09

import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author zhangpeng
 * @Date 2023 01 13 16 18
 **/

//定义相关样例类
case class Action(userId:String,action:String)
case class Pattern(action1:String,action2:String)

object BroadcastStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //定义数据流，读取用户行为事件
    val actionStream = env.fromElements(
      Action("zp", "login"),
      Action("zp", "pay"),
      Action("czx", "login"),
      Action("czx", "buy")
    )

    //定义规则流，读取指定的行为模式
    val patternStream = env.fromElements(
      Pattern("login", "pay"),
      Pattern("login", "buy")
    )

    //定义广播状态的描述器
    val patterns = new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern])
    //定义一条广播流
    val broadcastStream = patternStream.broadcast(patterns)

    //连接两条流进行处理
    actionStream.keyBy(_.userId)
      .connect(broadcastStream)
      .process(new PatternEvaluation)
      .print()

    env.execute()
  }

  //实现自定义的KeyedBroadcastProcessFunction
  class PatternEvaluation extends KeyedBroadcastProcessFunction[String,Action,Pattern,(String,Pattern)]{

    //定义值状态，保存用户上一次的行为
    lazy val prevActionState = getRuntimeContext.getState(new ValueStateDescriptor[String]("prev-action",classOf[String]))

    //处理数据流
    override def processElement(in1: Action, readOnlyContext: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)]#ReadOnlyContext, collector: Collector[(String, Pattern)]): Unit = {

      //从广播状态中获取行为模板
      val pattern = readOnlyContext.getBroadcastState(new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern]))
        .get(Unit)
      //从值状态中获取上次行为
      val prevAction = prevActionState.value()

      if (pattern != null && prevAction != null){
        if (pattern.action1 == prevAction && pattern.action2 == in1.action)
          collector.collect((readOnlyContext.getCurrentKey,pattern))
      }
      //保存状态
      prevActionState.update(in1.action)
    }

    //处理广播流
    override def processBroadcastElement(in2: Pattern, context: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)]#Context, collector: Collector[(String, Pattern)]): Unit = {
      val bcState = context.getBroadcastState(new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern]))
      bcState.put(Unit,in2)
    }
  }
}
