package com.zp.chapter09

import com.zp.chapter05.{ClickSource, Event}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
object KeyedStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .flatMap(new Myflatmap)
      .print()

    env.execute()
  }
  //自定义一个RichMapFunction
  class Myflatmap extends RichFlatMapFunction[Event,String]{
    //声明一个值状态
    var valueState: ValueState[Event] = _
    //声明一个列表状态
    var listState: ListState[Event] = _
    //声明一个map状态
    var mapState: MapState[String,Long] = _
    //声明一个归约状态
    var reducingState: ReducingState[Event] = _
    //声明一个聚合状态
    var aggState: AggregatingState[Event,String] = _


    //在open()生命周期中为值状态赋值
    override def open(parameters: Configuration): Unit = {
      //配置TTL,目前Flink只支持处理时间
      val config = StateTtlConfig.newBuilder(Time.seconds(10))
        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build()

      val stateDescriptor = new ValueStateDescriptor[Event]("my-value", classOf[Event])
      //TTL的配置是针对于StateDescriptor
      stateDescriptor.enableTimeToLive(config)


      valueState = getRuntimeContext.getState(stateDescriptor)
      listState = getRuntimeContext.getListState(new ListStateDescriptor[Event](("my-list"),classOf[Event]))
      mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("my-map",classOf[String],classOf[Long]))
      reducingState = getRuntimeContext.getReducingState(new ReducingStateDescriptor[Event]("my-reducing",
        new ReduceFunction[Event] {
          override def reduce(t: Event, t1: Event): Event = Event(t.user,t.url,t1.timestamp) //更新成最新事件毫无意义
        },classOf[Event]
      ))
      aggState = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[Event,Long,String]("my-agg",
        new AggregateFunction[Event,Long,String] {
          override def createAccumulator(): Long = 0L
          //需求就是统计模拟数据用户的访问次数
          override def add(in: Event, acc: Long): Long = acc + 1L

          override def getResult(acc: Long): String ="聚合状态为："+ acc

          override def merge(acc: Long, acc1: Long): Long = ???
        },classOf[Long] //这里是中间状态值的类型
      ))
    }

    override def flatMap(in: Event, collector: Collector[String]): Unit = {
      //对状态进行操作
//      println("值状态为："+valueState.value())
      valueState.update(in)
//      println("值状态为："+valueState.value())

      //对list进行操作
      listState.add(in)

      //对map进行操作
      if (mapState.contains(in.user)){
        //获取该用户的访问次数
        val l = mapState.get(in.user)
        mapState.put(in.user,l+1L)
      } else {
        mapState.put(in.user,1L)
      }

      println(s"用户${in.user}的访问次数为${mapState.get(in.user)}")

      println("------------------")
      reducingState.add(in)
      println(reducingState.get())
      println("------------------")
      aggState.add(in)
      println(aggState.get())
      println("------------------")
      println("------------------")


    }
  }
}
