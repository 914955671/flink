package com.pxd.TableAPI.UDF

import com.pxd.TableAPI.Test
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object AggregateFunctionT {
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


    //1.1 table api
    val avgTemp = new AvgTemp
    val rstTable = table
      .groupBy('field_1)
      .aggregate(avgTemp('field_3) as 'avgt)
      .select('field_1,'avgt)

    //2.1 sql
    tableEnv.createTemporaryView("table1",table)
    tableEnv.registerFunction("avgTemp",avgTemp)
    val rstSql = tableEnv.sqlQuery(
      """
        |select field_1,avgTemp(field_3)
        |from table1
        |group by field_1
        |""".stripMargin)

    rstTable.toRetractStream[Row].print("rst")
    rstSql.toRetractStream[Row].print("sql")

    env.execute("agg")
  }
}

//定义一个类，专门用于表示聚合的状态
class AvgTempAcc{
  var sum: Double = 0.0
  var count: Int = 0
}


//自定义一个聚合函数，求平均值,保存状态（tempsum,tempcount)
class AvgTemp extends  AggregateFunction[Double,AvgTempAcc]{
  override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum / accumulator.count

  override def createAccumulator(): AvgTempAcc = new AvgTempAcc

  //还要实现一个具体的处理计算函数，accumulate
  def accumulate(accumulator : AvgTempAcc, field_3 : Double): Unit = {
    accumulator.sum += field_3
    accumulator.count += 1
  }
}
