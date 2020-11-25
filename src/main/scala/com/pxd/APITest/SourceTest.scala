package com.pxd.APITest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer011}

object SourceTest {
  case class Sensorreading(id:String, timestamp:Long, temperatur: Double)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val ds: DataStream[Sensorreading] = env.fromCollection(List(
//      Sensorreading("sensor_1", 20201027, 20.2),
//      Sensorreading("sensor_2", 20201029, 20.3),
//      Sensorreading("sensor_3", 20201022, 20.4),
//      Sensorreading("sensor_4", 20201021, 20.5)
//    ))
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers","localhost:9092")
//    properties.setProperty("zookeeper.connect","localhost:2181")
//    val ds: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))
//    val ds: DataStream[String] = env.fromCollection(List("a b c","c b c","a c a","b s b"))
//    ds.flatMap(_.split(" ")).map((_,1)).filter(_._1 != "a").keyBy(0).sum(1).print()
//    ds.print()
    env.execute()
  }
}
