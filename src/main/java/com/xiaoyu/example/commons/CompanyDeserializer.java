package com.xiaoyu.example.commons;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Created By Have
 * 2024/11/2 0:41
 */
public class CompanyDeserializer implements Deserializer<Company> {
    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 0) {
            throw new SerializationException("Size of data received by " +
                    "CompanyDeserializer is shorter than expected");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name, address;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);
        name = new String(nameBytes, StandardCharsets.UTF_8);
        address = new String(addressBytes, StandardCharsets.UTF_8);
        return new Company(name, address);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }
}
