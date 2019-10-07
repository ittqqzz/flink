package com.tqz.java.kafka;

import com.tqz.java.kafka.entity.Company;
import com.tqz.java.kafka.serializer.CompanySerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {
    private static String HOST = "120.79.241.167:9092";
    private static String TOPIC = "topic-demo";

    public static void main(String[] args) {
        // 配置生产者参数，以下三个是必须的
        Properties properties = new Properties();
        // 1. 指定 key 和 value 的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        // 2. 指定生产者客户端连接 kafka 集群所需要的 broker 地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);


        // 创建生产者客户端
//        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);

        // 构建所需要发送的消息
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "Hello Kafka");
        ProducerRecord<String, Company> record = new ProducerRecord<>(TOPIC, new Company("Alibaba", "杭州"));

        // 发送消息
        producer.send(record);

        // 关闭连接
        producer.close();
    }
}
