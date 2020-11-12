package com.pxd.sink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object EsSink {
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
    //定义httphosts
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost",9200))
    //定义写入es的essinkfunction
    val myEsSinkFunc: ElasticsearchSinkFunction[Test] = new ElasticsearchSinkFunction[Test] {
      override def process(t: Test, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("field_1", t.field_1)
        dataSource.put("field_2", t.field_2.toString)
        dataSource.put("field_3", t.field_3.toString)

        val indexRequest: IndexRequest = Requests.indexRequest()
          .index("test")
          .`type`("type")
          .source(dataSource)
        requestIndexer.add(indexRequest)
      }
    }

    dsTest.addSink(new ElasticsearchSink.Builder[Test](httpHosts,myEsSinkFunc).build())

    env.execute("ES Sink Test")
  }
}
