package com.pxd.TableAPI.UDF

import com.pxd.TableAPI.Test
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionT {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//设置时间特性

    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val ds: DataStream[String] = env.readTextFile("D:\\IDEA\\Project\\flink\\src\\main\\scala\\com\\pxd\\text.txt")
    val dsTest: DataStream[Test] = ds.map {
      textContent => {
        val str: Array[String] = textContent.split(",")
        Test(str(0),str(1).toLong,str(2).toDouble)
      }
    }
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Test](Time.seconds(1)) {
        override def extractTimestamp(t: Test):Long = t.field_2 * 1000L
      })

    val table: Table = tableEnv.fromDataStream(dsTest, 'field_1,'field_2.rowtime as 'ts,'field_3)

    //1. table api
    val split = new Split("_") //new 一个UDF实例
    val rst = table
      .joinLateral( split('field_1) as ('word,'length))
      .select('field_1,'ts,'word,'length)

    //2. sql api
    tableEnv.createTemporaryView("tableT",table)
    tableEnv.registerFunction("split",split)
    val rstSql = tableEnv.sqlQuery(
      """
        |select
        |  field_1,
        |  ts,
        |  word,
        |  length
        |from
        |  tableT, lateral table( split(field_1) ) as splitid(word,length)
        |""".stripMargin)

    rst.toAppendStream[Row].print("rst")
    rstSql.toAppendStream[Row].print("rstSql")

    env.execute("table function test")
  }
}

//自定义Table Function
class Split( separator : String) extends  TableFunction[(String,Int)]{
  def eval(str : String): Unit ={
    str.split(separator).foreach(
      word => collect((word,word.length))
    )
  }
}
