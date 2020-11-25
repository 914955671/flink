package com.pxd.TableAPI

import com.pxd.sink.Test
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._


object T1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[String] = env.readTextFile("D:\\IDEA\\Project\\flink\\src\\main\\scala\\com\\pxd\\text.txt")
    val dsTest: DataStream[Test] = ds.map {
      textContent => {
        val str: Array[String] = textContent.split(",")
        Test(str(0),str(1).toInt,str(2).toDouble)
      }
    }

    //首先创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)
    //基于流创建一张表
    val dataTable = tableEnv.fromDataStream(dsTest)
    //调用table api进行转换
    var resultTable: Table = dataTable.select("field_1,field_3").filter("field_3 > 100")


    //直接用sql实现
    tableEnv.createTemporaryView("DataTable",dataTable)
    val sql:String = "select field_1,field_3 from DataTable"
    val resultSqlTable: Table = tableEnv.sqlQuery(sql)

    resultTable.toAppendStream[(String,Double)].print("result")
    resultSqlTable.toAppendStream[(String,Double)].print("sqlresult")
    env.execute("JDBC Sink Test")
  }
}
