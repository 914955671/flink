package com.pxd.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
/**
 * 批处理的scala
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1. 创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据
    val filePath = "D:\\IntelliJ IDEA Community Edition 2020.2.2\\Projects\\FlinkStudy\\src\\main\\resources\\hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(filePath)
    val result: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    result.print()
  }
}
