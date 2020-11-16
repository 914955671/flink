package com.pxd.state

import java.util

import com.pxd.sink.Test
import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._




object StateTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[String] = env.socketTextStream("localhost", 7777)
    val dsTest: DataStream[Test] = ds.map {
      textContent => {
        val str: Array[String] = textContent.split(",")
        Test(str(0),str(1).toInt,str(2).toDouble)
      }
    }




    env.execute("State Test")
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