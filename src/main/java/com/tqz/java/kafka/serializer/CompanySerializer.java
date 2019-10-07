package com.tqz.java.kafka.serializer;

import com.tqz.java.kafka.entity.Company;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 当发出去的消息是自定义数据类型的话，需要使用自定义的序列化器，序列化算法可以用开源技术
 */
public class CompanySerializer implements Serializer<Company> {
    /**
     * 在创建 producer 前设置编码，默认 UTF-8 所以一般不管它
     *
     * @param configs
     * @param isKey
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    /**
     * 执行序列化操作
     *
     * @param topic
     * @param data
     * @return
     */
    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }
        byte[] name;
        byte[] address;
        try {
            if (data.getName() != null) {
                name = data.getName().getBytes("UTF-8");
            } else {
                name = new byte[0];
            }
            if (data.getAddress() != null) {
                address = data.getAddress().getBytes("UTF-8");
            } else {
                address = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    /**
     * 关闭当前的序列化器，一般不用管它。
     * 若修改它，必须保证幂等性
     */
    @Override
    public void close() {

    }
}
