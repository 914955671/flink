package com.pxd.TableAPI
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.java.BatchTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}
import org.apache.flink.table.types.DataType


object T2 {
  def main(args: Array[String]): Unit = {
    //1. 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2. 定义表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
/*

    //2.1 基于老版本planner的流处理
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    StreamTableEnvironment.create(env,settings)

    //2.2 基于老版本的批处理
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)


    //2.3 基于blink planner的流处理
    val blinkStreamSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamSettings)

    //2.4
    val blinkBatchSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val blinkBatchTableEnv: TableEnvironment = TableEnvironment.create(blinkBatchSettings)
*/

    //3. 连接外部系统，读取数据，注册表
    //3.1 读取文件
    val filePath = "D:\\IDEA\\Project\\flink\\src\\main\\scala\\com\\pxd\\text.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
                .field("field_1",DataTypes.STRING())
                .field("field_2",DataTypes.INT())
                .field("field_3",DataTypes.DOUBLE())
      ).createTemporaryTable("inputTable")

    //3.2 从kafka中读取数据
    tableEnv.connect(new Kafka()
      .version("2.6.0")
      .topic("test")
      .property("zookeeper.connect","localhost:2181")
      .property("boostrap.servers","localhost:9092")
    ).withFormat(new Csv())
      .withSchema(new Schema()
        .field("field_1",DataTypes.STRING())
        .field("field_2",DataTypes.INT())
        .field("field_3",DataTypes.DOUBLE())
      ).createTemporaryTable("KafkaTable")


    val inputTable: Table = tableEnv.from("inputTable")
    val KafkaTable: Table = tableEnv.from("KafkaTable")

//    inputTable.toAppendStream[(String, Int, Double)].print()
    KafkaTable.toAppendStream[(String, Int, Double)].print()
    env.execute("Table API Test")

  }
}
