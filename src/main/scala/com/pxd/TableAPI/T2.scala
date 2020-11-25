package com.pxd.TableAPI
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.java.BatchTableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment


object T2 {
  def main(args: Array[String]): Unit = {
    //1. 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2. 定义表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

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
    val blinkBatchTableEnv: StreamTableEnvironment = TableEnvironment.create(blinkBatchSettings)




  }
}
