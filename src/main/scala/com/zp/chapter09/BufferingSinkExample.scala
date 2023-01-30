package com.zp.chapter09

import com.zp.chapter05.{ClickSource, Event}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * @Author zhangpeng
 * @Date 2023 01 13 15 18
 *      算子状态
 **/
object BufferingSinkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .addSink(new BufferingSink(10))

    env.execute()
  }
  //实现自定义的SinkFunction
  class BufferingSink(threshold: Int) extends SinkFunction[Event] with CheckpointedFunction{

    //定义列表状态，保存要缓冲的数据
    var bufferedState: ListState[Event] = _
    //定义一个本地变量列表,便于对于缓冲数据的处理,将数据同步到bufferedState中
    val bufferedList = ListBuffer[Event]()


    override def invoke(value: Event, context: SinkFunction.Context): Unit = {
      //缓冲数据
      bufferedList += value

      //判断是否达到了阈值
      if (bufferedList.size == threshold){
        //输出到外部系统，用打印到控制台模拟
        bufferedList.foreach(data => println(data))
        println("=================输出完毕=====================")

        //清空缓存
        bufferedList.clear()
      }
    }
    //将数据缓存到ListState中
    override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
      //清空状态
      bufferedState.clear()
        for (data <- bufferedList){
          bufferedState.add(data)
        }
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
      bufferedState = context.getOperatorStateStore.getListState(new ListStateDescriptor[Event]("buffered-list",classOf[Event]))

      //判断如果是从故障中恢复，那么就将状态中的数据添加到局部变量中
      if (context.isRestored){
        import scala.collection.convert.ImplicitConversions._
        for (data <- bufferedState.get()){
          bufferedList += data
        }
      }
    }
  }
}
