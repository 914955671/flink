package com.pxd.TableAPI

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object T4 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(5)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //Kafka 写入
    tableEnv.connect(new Kafka()
      .version("universal")
      .property("bootstrap.servers","127.0.0.1:9092")
      .property("zookeeper.connection","127.0.0.1:2181")
      .topic("test")
    ).withFormat(new Csv())
      .withSchema(new Schema()
        .field("field_1",DataTypes.STRING())
        .field("field_2",DataTypes.INT())
        .field("field_3",DataTypes.DOUBLE())
      ).createTemporaryTable("InputTable")
    //Kafka写出
    tableEnv.connect(new Kafka()
      .version("universal")
      .property("bootstrap.servers","127.0.0.1:9092")
      .property("zookeeper.connection","127.0.0.1:2181")
      .topic("test2")
    ).withFormat(new Csv())
      .withSchema(new Schema()
        .field("field_1",DataTypes.STRING())
//        .field("field_2",DataTypes.INT())
        .field("field_2",DataTypes.DOUBLE())
      ).createTemporaryTable("OutputTable")

    //获取写入表
    val inputTable: Table = tableEnv.from("InputTable")

    //获取写出表
    val outputTable: Table = tableEnv.from("OutputTable")

    val rstTable: Table = inputTable
      .select('field_1, 'field_3)
      .filter('field_1 === "test_1" )

    //从写入表写到写出表
    rstTable.insertInto("OutputTable")
    rstTable.toAppendStream[(String,Double)].print()
    env.execute("Kafka Test")
  }
}
