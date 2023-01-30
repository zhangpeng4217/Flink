package com.zp.chapter08
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
object BillCheckExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 来自 app 的支付日志
    val appStream = env
      .fromElements(
        ("order-1", "app", 1000L),
        ("order-2", "app", 2000L)
      )
      .assignAscendingTimestamps(_._3)
    // 来自第三方支付平台的支付日志
    val thirdPartyStream = env
      .fromElements(
        ("order-1", "success", "wechat", 3000L),
        ("order-3", "success", "alipay", 4000L)
      )
      .assignAscendingTimestamps(_._4)
    // 检测同一支付单在两条流中是否匹配，不匹配就报警
    appStream.connect(thirdPartyStream)
      .keyBy(_._1,_._1)
      .process(new CoProcessFunction[(String,String,Long),(String,String,String,Long),String] {
         //定义状态变量，用来保存已经到达的事件
//        var appEvent = ValueState[(String,String,Long)] = _
//        var thirdpartyEvent = ValueState[(String,String,String,Long)] = _
//
//
//        override def open(parameters: Configuration): Unit = {
//          appEvent = getRuntimeContext.getState(new ValueStateDescriptor[(String,String,Long)]("app-event",classOf[(String,String,Long)]))
//          thirdpartyEvent = getRuntimeContext.getState(new ValueStateDescriptor[(String,String,String,Long)]("thirdparty-event",classOf[(String,String,String,Long)]))
//        }
// 定义状态变量，用来保存已经到达的事件；使用 lazy 定义是一种简洁的写法
        lazy val appEvent = getRuntimeContext.getState(
          new ValueStateDescriptor[(String, String, Long)]("app", classOf[(String, String, Long)]))

        lazy val thirdPartyEvent = getRuntimeContext.getState(
          new ValueStateDescriptor[(String, String, String, Long)]("third-party", classOf[(String, String, String, Long)]))

        //每来一条数据都要
        override def processElement1(in1: (String, String, Long), context: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#Context, collector: Collector[String]): Unit = {
          //获取相关需要的元素,对其相关处理
          if (thirdPartyEvent != null){
            // 如果对应的第三方支付事件的状态变量不为空，则说明第三方支付事件先到达，对账成功
            collector.collect(in1._1 + " 对账成功")
            // 清空保存第三方支付事件的状态变量
            thirdPartyEvent.clear()
          } else {
            // 如果是 app 支付事件先到达，就把它保存在状态中
            appEvent.update(in1)
            //如果另一条流中事件没有到达，那就注册定时器，开始等待 等待5秒
            context.timerService().registerEventTimeTimer(in1._3+5000)
          }
        }
        //和上面逻辑是对称关系
        override def processElement2(in2: (String, String, String, Long), context: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#Context, collector: Collector[String]): Unit = {
          //获取相关需要的元素,对其相关处理
          if (appEvent != null){
            // 如果对应的第APP支付事件的状态变量不为空，则说明第APP支付事件先到达，对账成功
            collector.collect(in2._1 + " 对账成功")
            // 清空保存第APP支付事件的状态变量
            appEvent.clear()
          } else {
            // 如果是 第三方 支付事件先到达，就把它保存在状态中
            thirdPartyEvent.update(in2)
            //如果另一条流中事件没有到达，那就注册定时器，开始等待 等待5秒
            context.timerService().registerEventTimeTimer(in2._4+5000)

          }
        }
        override def onTimer(timestamp: Long, ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#OnTimerContext, out: Collector[String]): Unit ={
          // 如果 app 事件的状态变量不为空，说明等待了 5 秒钟，第三方支付事件没有到达
          if (appEvent.value() != null) {
            out.collect(appEvent.value()._1 + " 对账失败，订单的第三方支付信息未到")
            appEvent.clear()
          }
          // 如果第三方支付事件没有到达，说明等待了 5 秒钟，app 事件没有到达
          if (thirdPartyEvent.value() != null) {
            out.collect(thirdPartyEvent.value()._1 + " 对账失败，订单的 app 支付信息未到")
            thirdPartyEvent.clear()
          }
        }
      })
        .print()
    env.execute()
  }

}
