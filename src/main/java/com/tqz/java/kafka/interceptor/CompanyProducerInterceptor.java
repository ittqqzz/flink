package com.tqz.java.kafka.interceptor;

import com.tqz.java.kafka.entity.Company;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CompanyProducerInterceptor implements ProducerInterceptor<String, Company> {
    /**
     * 在序列化与计算分区之前执行此方法
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, Company> onSend(ProducerRecord<String, Company> record) {
        Company company = record.value();
        company.setAddress(company.getAddress() + "拦截器修改后");
        return new ProducerRecord<>(record.topic(),
                record.partition(), record.timestamp(),
                record.key(), company, record.headers());
    }

    /**
     * 该方法会在 kafka 应答(acknowledged)之前或消息发送失败时执行
     * 优先于用户给 send 的回调函数
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            System.out.println("拦截修改成功: " + metadata.toString());
        } else {
            System.out.println("拦截失败：" + exception.getMessage());
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
