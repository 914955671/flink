package com.pxd.TableAPI

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row



case class Test(field_1:String,field_2:Long,field_3:Double)
/**
 * Time And Window Test
 */
object T6 {

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

    //1. Group Window
    //1.1 table api
    val rstTable =
      table.window(Tumble over 10.seconds on 'ts as 'tw) //每10s统计一次，滚动时间窗口
        .groupBy('field_1,'tw)
        .select('field_1,'field_1.count, 'field_3.avg, 'tw.end)

    //2. sql
    tableEnv.createTemporaryView("test", table)
    val rstSqlTable = tableEnv.sqlQuery(
      """
        |select
        |  field_1,
        |  count(field_1),
        |  avg(field_3),
        |  tumble_end(ts, interval '10' second)
        |from test
        |group by
        |  field_1,
        |  tumble(ts, interval '10' second)
        |""".stripMargin)

    //2. Over window：统计每个field_1 每条数据，与之前两行数据的平均温度。
    //2.1 table api
    val overrstTable = table
      .window( Over.partitionBy("field_1").orderBy('ts).preceding(2.rows).as('ow))
      .select('field_1,'ts,'field_1.count over 'ow,'field_3.avg over 'ow)

    //2.2 sql
    val overrstSqlTable = tableEnv.sqlQuery(
      """
        |select
        |  field_1,
        |  ts,
        |  count(field_1) over ow,
        |  avg(field_3) over ow
        |from test
        |window ow as (
        |  partition by field_1
        |  order by ts
        |  rows between 2 preceding and current row
        |)
        |""".stripMargin)

    //转换成流打印输出
    overrstTable.toRetractStream[Row].print("result")
    overrstSqlTable.toRetractStream[Row].print("Sql Rst")

    //    table.printSchema()
//    table.toAppendStream[Row].print()

    env.execute("Time And Window Test")
  }
}
