package com.tqz.scala.course08

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaConnectorProducerApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("10.101.50.10", 9999)
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "10.101.50.10:9092")
    val kafkaSink = new FlinkKafkaProducer[String](
      "10.101.50.10:9092", // broker list
      "pktest", // target topic
      new SimpleStringSchema)
    kafkaSink.setWriteTimestampToKafka(true)
    data.addSink(kafkaSink)
    env.execute("KafkaConnectorProducerApp")
  }

}
