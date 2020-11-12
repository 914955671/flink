package com.pxd.sink
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object MySqlSink {

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
    dsTest.addSink(new MyJDBCSinkFunc())
    env.execute("JDBC Sink Test")
  }
}

class MyJDBCSinkFunc() extends RichSinkFunction[Test]{
  //定义连接，预编译语句
  var conn:Connection = _
  var insertStmt:PreparedStatement = _
  var updateStmt:PreparedStatement = _


  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456")
    insertStmt = conn.prepareStatement("insert into test values (?,?)")
    updateStmt = conn.prepareStatement("update test set field_3 = ? where field_1 = ?")
  }

  override def invoke(value: Test, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1,value.field_3)
    updateStmt.setString(2,value.field_1)
    updateStmt.execute()

    if( updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.field_1)
      insertStmt.setDouble(2,value.field_3)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    conn.close()
    insertStmt.close()
    updateStmt.close()
  }
}