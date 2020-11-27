package com.pxd.TableAPI

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{AggregatedTable, DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Schema}


object T5 {
  def main(args: Array[String]): Unit = {
    //获取环境变量
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //文本文件存放位置
    val filePath: String = "D:\\IntelliJ IDEA Community Edition 2020.2.2\\Projects\\FlinkStudy\\src\\main\\scala\\com\\pxd\\text.txt"
    //注册输入表相关表信息
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("field_1",DataTypes.STRING())
        .field("field_2",DataTypes.INT())
        .field("field_3",DataTypes.DOUBLE())
      ).createTemporaryTable("InputTable")
    //注册输出表
    tableEnv.connect(
      new Elasticsearch()
        .version("6")
        .host("127.0.0.1",9200,"http")
        .index("flink_sink_test")
        .documentType("field_3")
    ).inUpsertMode()
      .withFormat(new Json())
      .withSchema(
        new Schema()
          .field("field_1",DataTypes.STRING())
          .field("count",DataTypes.BIGINT())
      )
      .createTemporaryTable("ESOutputTable")


    //获取表
    val inputTable: Table = tableEnv.from("InputTable")
    //按照field_1聚合
    val rstTable: Table = inputTable
      .groupBy('field_1)
      .select('field_1, 'field_1.count as 'count)

    rstTable.insertInto("ESOutputTable")

    env.execute(" T5 Table Test")

  }
}
