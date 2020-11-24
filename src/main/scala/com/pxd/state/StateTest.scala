package com.pxd.state

import java.util

import com.pxd.sink.Test
import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector




object StateTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new FsStateBackend("hdfs://test"))
    //启用checkpoint，传入的参数为间隔时间
    env.enableCheckpointing(1000)
    //设置checkpoint参数
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    //checkpoint时间，设置一分钟。
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //最大同时存在的checkpoint数量
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //最小的间隔时间
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    //是否更倾向于用checkpoint做故障恢复
    env.getCheckpointConfig.setPreferCheckpointForRecovery(false)
    //容忍checkpoint失败次数
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(1)
    //配置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000))

    env.setParallelism(1)
    val ds: DataStream[String] = env.socketTextStream("localhost", 7777)
    val dsTest: DataStream[Test] = ds.map {
      textContent => {
        val str: Array[String] = textContent.split(",")
        Test(str(0),str(1).toInt,str(2).toDouble)
      }
    }
      .keyBy(_.field_1)
      .process(new MyProcessFunction)
//      .flatMap(new MyRichFlatMapper())


    env.execute("State Test")
  }
}

class MyProcessFunction extends KeyedProcessFunction[String, Test, String]{

  var keystate:ValueState[Test] = _


  override def open(parameters: Configuration): Unit = {
    keystate = getRuntimeContext.getState(new ValueStateDescriptor[Double]("State",classOf[Double]))
    super.open(parameters)
  }

  override def processElement(i: Test, context: KeyedProcessFunction[String, Test, String]#Context, collector: Collector[String]): Unit = {
    context.getCurrentKey
    context.timestamp()
    context.timerService().registerEventTimeTimer(context.timestamp() + 10000L)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Test, String]#OnTimerContext, out: Collector[String]): Unit = {

    print("111111111111111111111")
  }
}


//状态编程
class MyRichFlatMapper() extends RichFlatMapFunction[Test,(String,Double,Double)]{

  private val flatMapState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("FlatMap State", classOf[Double]))

  override def flatMap(value: Test, out: Collector[(String, Double, Double)]): Unit = {
    val last: Double = flatMapState.value()
    val diff: Double = (value.field_3 - last).abs
    if(diff > 10){
      out.collect(value.field_1,last,value.field_3)
    }

    flatMapState.update(value.field_3)

  }
}



class MyReduceFunction extends ReduceFunction[Test]{
  override def reduce(value1: Test, value2: Test): (String,Double) = {
    (value1.field_1,value1.field_3 + value2.field_3)
  }
}

class MyRichMapper extends RichMapFunction[Test,String]{
  //状态
  var lastTemp:ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("liststate", classOf[Int]))
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("", classOf[String], classOf[Double]))
  lazy val reduceState: ReducingState[Test] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[Test]("reduceState", new MyReduceFunction, classOf[Test]))


  override def open(parameters: Configuration): Unit = {
    lastTemp = getRuntimeContext.getState[Double]{
      new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
    }
  }

  override def map(in: Test): String = {
    //状态读写
    lastTemp.value()
    lastTemp.update(12.3)

    listState.add(1)

    in.field_1
  }
}