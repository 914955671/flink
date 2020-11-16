package com.pxd.sink

import com.pxd.APITest
import com.pxd.APITest.Test
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

//定义一个RedisMapper
class MyRedisMapper extends RedisMapper[Test]{
  //定义保存数据写入redis的命令,HSet 表名 key value
  override def getCommandDescription: RedisCommandDescription =
     new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")

  //将field_3指定为value
  override def getKeyFromData(t: Test): String = t.field_3.toString

  //将field_1指定为key
  override def getValueFromData(t: Test): String = t.field_1
}

object redissink {
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
    //定义一个FlinkJedisConfigBase
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("127.0.0.1")
      .setPort(6379)
      .setPassword("123456")
      .build()


    dsTest.addSink(new RedisSink[Test](config,new MyRedisMapper))
    env.execute("redis sink test")
  }
}
