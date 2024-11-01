package com.xiaoyu;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;

/**
 * @author chy
 * @description
 * @date 2024/11/1
 */
public class ProducerFactory {
    public static KafkaProducer<String, String> getStringProducer(String serverUrl) {
        HashMap<String, Object> config = new HashMap<>();

        // 连接地址
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);

        // ACK
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        // 相应超时.
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5000);

        // 缓冲区大小. (发送给服务器的)
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024 * 1024 * 10);
        // 每次最多发10K
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1024 * 10);
        // 不重试,有些非幂等性可以.
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        // snappy 压缩..
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // 序列化
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(config);
    }
}
