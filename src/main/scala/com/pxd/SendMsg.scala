package com.pxd

import java.io.{BufferedReader, InputStreamReader, OutputStream, PrintStream}
import java.net.{ServerSocket, Socket}

object SendMsg {
  def main(args: Array[String]): Unit = {
    //创建服务端
    val serverSocket = new ServerSocket(12345)
    //服务端的接手端口
    val socket: Socket = serverSocket.accept()
    //获取该接收端的输出流
    val outputStream: OutputStream = socket.getOutputStream
    //包装输出流
    val os = new PrintStream(outputStream)
    //读取控制台输入信息
    val reader = new BufferedReader(new InputStreamReader(System.in))
    while (1 == 1){
      //从控制台获取信息
      val str: String = reader.readLine()
      //将信息打印出去
      os.println(str)
    }
  }
}
