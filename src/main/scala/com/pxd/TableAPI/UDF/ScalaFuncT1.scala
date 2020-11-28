package com.pxd.TableAPI.UDF

import com.pxd.TableAPI.Test
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalaFuncT1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//设置时间特性

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

    //调用自定义hash函数，对filed_1 进行hash运算
    //1. table api
    //首先 new 一个 UDF 的实例
    val hashCode = new HashCode(23)
    val rst = table
      .select('field_1,'ts,hashCode('field_1))

    //2. sql
    // 需要在环境中注册UDF
    tableEnv.createTemporaryView("udf_table",table)
    tableEnv.registerFunction("hashcode", hashCode)
    val rstSql = tableEnv.sqlQuery("select field_1,ts,hashcode(field_1) from udf_table")

    rst.toAppendStream[Row].print("rst")
    rstSql.toAppendStream[Row].print("sqlRst")

    env.execute("UDF Test")
  }
}

//自定义标量函数
class HashCode( factor : Int ) extends  ScalarFunction{
  def eval( s : String ) : Int ={
    s.hashCode * factor - 10000
  }
}
