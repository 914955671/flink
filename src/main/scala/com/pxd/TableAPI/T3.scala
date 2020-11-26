package com.pxd.TableAPI

import com.pxd.sink.Test
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.types.DataType

object T3 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val filePath: String = "D:\\IntelliJ IDEA Community Edition 2020.2.2\\Projects\\FlinkStudy\\src\\main\\scala\\com\\pxd\\text.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("field_1",DataTypes.STRING())
        .field("field_2",DataTypes.INT())
        .field("field_3",DataTypes.DOUBLE())
        ).createTemporaryTable("InputTable")

    val testTable: Table = tableEnv.from("InputTable")
    //简单转换
    val rstTable: Table = testTable
      .select('field_1, 'field_3)
      .filter('field_1 === "test_1")
    //聚合转换
    val aggTable: Table = testTable
      .groupBy("field_1")
      .select('field_1, 'field_1.count as 'count)

    //输出表
    val resFilePath = "D:\\IntelliJ IDEA Community Edition 2020.2.2\\Projects\\FlinkStudy\\src\\main\\scala\\com\\pxd\\test1.txt"
    val outPutTable: Unit = tableEnv.connect(new FileSystem().path(resFilePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("field_1", DataTypes.STRING())
//        .field("field_2",DataTypes.INT())
        .field("field_3", DataTypes.BIGINT())
      ).createTemporaryTable("OutPutTable")

    rstTable.toAppendStream[(String,Double)].print("result")
//    aggTable.toRetractStream[(String,Long)].print("agg result")

    rstTable.insertInto("OutPutTable")

    env.execute("Table API")

  }
}
