package com.pxd.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
//流的wordcount
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //调整并行度
//    streamEnv.setParallelism(8)
    //从外部获取host和port
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = parameterTool.get("host")
    val port: Int = parameterTool.getInt("port")
    //与本机的12345端口建立连接
    val ds: DataStream[String] = streamEnv.socketTextStream(host, port)
    //计算
    val wc: DataStream[(String, Int)] = ds.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)
    //输出结果
    wc.print().setParallelism(1)
    //让数据流处于运行状态
    streamEnv.execute()
  }
}
