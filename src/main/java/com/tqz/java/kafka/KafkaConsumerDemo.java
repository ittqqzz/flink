package com.tqz.java.kafka;

import com.tqz.java.kafka.deserializer.CompanyDeserializer;
import com.tqz.java.kafka.entity.Company;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.List;
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
//        consumer.subscribe(Collections.singletonList(TOPIC));

        // 消费指定分区的消息
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        consumer.assign(Arrays.asList(tp));
        long lastConsumedRecord = -1; // 当前正在消费的位移

        // 消费消息
        while (true) {
            ConsumerRecords<String, Company> records = consumer.poll(1000);
            if (records.isEmpty()) {
                break;
            }
            // 获取指定分区里面的消息
            List<ConsumerRecord<String, Company>> partitionRecords = records.records(tp);
            // 获取最后消费的消息的 offset
            lastConsumedRecord = partitionRecords.get(partitionRecords.size() - 1).offset();
            // 同步提交消费位移
            consumer.commitSync();

//            for (ConsumerRecord<String, Company> record : records) {
//                System.out.println(record.value());
//            }
        }

        /**
         * 说明：
         * 这里测试的是：lastConsumedRecord、commit offset、position 之间的关系
         *
         * 不开启生产者，第一次运行输出 -1、8、8
         * 开启生产者后，再运行输出 8、9、9
         *
         * -1、8、8 说明本次应该从 offset 为 8 的位置开始消费，但是没有消费到消息
         * 8、9、9 说明本次是从 offset 为 8 的位置消费消息的，且已经提交 offset 了，下一次拉取的消息的位置是 position = 8
         *
         * 但是 position 不是一直等于 commit offset 的
         */
        System.out.println(lastConsumedRecord);
        OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
        System.out.println(offsetAndMetadata.offset());
        long position = consumer.position(tp);
        System.out.println(position);
    }
}
