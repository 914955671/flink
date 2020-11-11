package com.pxd.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink

object EsSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[String] = env.readTextFile("D:\\IntelliJ IDEA Community Edition 2020.2.2\\Projects\\FlinkStudy\\src\\main\\scala\\com\\pxd\\text.txt")
    val dsTest: DataStream[Test] = ds.map {
      textContent => {
        val str: Array[String] = textContent.split(",")
        Test(str(0),str(1).toInt,str(2).toDouble)
      }
    }
    dsTest.addSink(new ElasticsearchSink.Builder[Test](httpHosts,myEsSinkFunc).build())

    env.execute("ES Sink Test")
  }
}
