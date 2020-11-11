package com.pxd.sink

import java.util.Properties

import com.pxd.APITest.Test
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
object KafkaSink {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //写入信息
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","127.0.0.1:9092")
    properties.setProperty("zookeeper.connect","127.0.0.1:2182")
    val ds: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))

    val dsTest: DataStream[String] = ds.map {
      textContent => {
        val str: Array[String] = textContent.split(",")
        Test(str(0),str(1).toInt,str(2).toDouble).toString
      }
    }
    dsTest.print()
    dsTest.addSink(new FlinkKafkaProducer[String]("localhost:9092","test",new SimpleStringSchema()))
    env.execute("kafka sink")
  }
}
