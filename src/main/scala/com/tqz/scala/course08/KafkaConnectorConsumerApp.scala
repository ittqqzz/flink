package com.tqz.scala.course08

import java.util.Properties
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaConnectorConsumerApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "120.79.241.167:9092")
    properties.setProperty("group.id", "test")
    val stream = env
      .addSource(new FlinkKafkaConsumer[String]("pktest", new SimpleStringSchema(), properties))
      .print()
    env.execute("KafkaConnectorConsumerApp")
  }
}
