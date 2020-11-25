package com.pxd.APITest

import org.apache.flink.api.common.functions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._




class MyRichMapper extends RichMapFunction[Test,String]{

  override def close(): Unit = {
    //一般做收尾工作，比如关闭连接，或者清空状态。
  }

  override def open(parameters: Configuration): Unit = {
    //做一些初始化操作，比如数据库地连接
    val context: RuntimeContext = getRuntimeContext

  }

  override def map(in: Test): String = ???
}

class MyFunc extends ReduceFunction[Test]{
  override def reduce(t: Test, t1: Test): Test = {
    Test(t.field_1,t.field_2.max(t1.field_2),t.field_3.min(t1.field_3))
  }
}
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[String] = env.readTextFile("..\\text.txt")
    val dsTest: DataStream[Test] = ds.map(
      textContent => {
        val str: Array[String] = textContent.split(",")
        Test(str(0),str(1).toInt,str(2).toDouble)
      }
    )
//    dsTest.print()
//    dsTest.keyBy("field_1").minBy("field_3").print()
//    dsTest.keyBy("field_1").reduce((lastTestData,newTestData) => {
//      Test(lastTestData.field_1,lastTestData.field_2.max(newTestData.field_2),lastTestData.field_3.min(newTestData.field_3))
//    }).print()
    val splitStream: SplitStream[Test] = dsTest.split(data => {
      if (data.field_3 > 100) Seq("high") else Seq("low")
    })
    val highStream: DataStream[Test] = splitStream.select("high")
    val lowStream: DataStream[Test] = splitStream.select("low")
    val allStream: DataStream[Test] = splitStream.select("all","low")

    highStream.print("high")
    lowStream.print("low")
    allStream.print("all")
    env.execute()
  }
}



