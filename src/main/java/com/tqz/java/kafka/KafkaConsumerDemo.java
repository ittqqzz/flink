package com.tqz.java.kafka;

import com.tqz.java.kafka.deserializer.CompanyDeserializer;
import com.tqz.java.kafka.entity.Company;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerDemo {
    private static String HOST = "120.79.241.167:9092";
    private static String TOPIC = "topic-demo";
    private static String GROUP_ID = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        // 设置消费组的名称
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // 创建消费者
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<>(properties);

        // 订阅主题
        consumer.subscribe(Collections.singletonList(TOPIC));

        // 消费消息
        while (true) {
            ConsumerRecords<String, Company> records = consumer.poll(1000);
            for (ConsumerRecord<String, Company> record : records) {
                System.out.println(record.value());
            }
        }
    }
}
