package com.xiaoyu.example.commons;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created By Have
 * 2024/11/3 14:50
 */
public class ProtostuffSerializer implements Serializer<Company> {
    @Override
    public byte[] serialize(String topic, Headers headers, Company data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public byte[] serialize(String s, Company company) {
        if (company == null) {
            return null;
        }
        Schema<Company> schema = RuntimeSchema.getSchema(Company.class);
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] protostuff = null;
        try {
            ProtostuffIOUtil.toByteArray(company, schema, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            buffer.clear();
        }
        return protostuff;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
