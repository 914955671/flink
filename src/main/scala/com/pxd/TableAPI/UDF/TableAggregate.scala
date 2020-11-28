package com.pxd.TableAPI.UDF

import com.pxd.TableAPI.Test
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggregate {
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

    //table api
    val top2T = new Top2T()
    val rst = table
      .groupBy('field_1)
      .flatAggregate(top2T('field_3) as ('tmp, 'rank))
      .select('field_1, 'tmp, 'rank)

    //sql api
    rst.toRetractStream[Row].print("rst")

    env.execute("agg Table")
  }
}

//定义一个类，用来表时表聚合函数的状态
class Top2TAcc{
  var h :Double = Double.MinValue
  var s :Double = Double.MinValue
}

//自定义表聚合函数，提取所有field_3中最高的两个值,(temp,rank)
class Top2T extends TableAggregateFunction[(Double,Int), Top2TAcc]{
  override def createAccumulator(): Top2TAcc = new Top2TAcc()

  //实现计算结果的函数accumulate

  def accumulate( acc:Top2TAcc, field_3:Double): Unit ={
    //判断当前的温度值是否比状态中的大
    if( field_3 > acc.h){
      //如果比最高温度还高，就排在第一，原来的第一顺到第二位
      acc.s = acc.h
      acc.h = field_3
    }else if( field_3 > acc.s){
      //如果在最高和第二高之间，那么直接替换第二稿
      acc.s = field_3
    }
  }

  //实现一个输出结果的方法，最终处理完表中所有数据值调用
  def emitValue( acc: Top2TAcc, out: Collector[(Double,Int)]): Unit ={
    out.collect((acc.h,1))
    out.collect((acc.s,2))
  }
}
