package com.pxd.sink

import com.pxd.APITest.Test
import org.apache.flink.streaming.api.scala._


case class Test(field_1:String,field_2:Int,field_3:Double)
object Test {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[String] = env.readTextFile("D:\\IntelliJ IDEA Community Edition 2020.2.2\\Projects\\FlinkStudy\\src\\main\\scala\\com\\pxd\\text.txt")
    val dsTest: DataStream[Test] = ds.map {
      textContent => {
        val str: Array[String] = textContent.split(",")
        Test(str(0),str(1).toInt,str(2).toDouble)
      }
    }
    dsTest.writeAsCsv("D:\\IntelliJ IDEA Community Edition 2020.2.2\\Projects\\FlinkStudy\\src\\main\\scala\\com\\pxd\\res.csv")
    env.execute()
  }
}
