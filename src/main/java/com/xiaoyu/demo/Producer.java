package com.xiaoyu.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.stream.IntStream;

public class Producer {

    public static void main(String[] args) throws InterruptedException {
         final String url = "124.220.233.169:9092";
		final String topic = "topic-1";
        // 配置.
        HashMap<String, Object> config = new HashMap<>();

        // 连接地址
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);

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

        // ok了.
        while (true) {
            Thread.sleep(1000);
            KafkaProducer<String, String> producer = new KafkaProducer<>(config);
            IntStream.range(0, 100).forEach(value -> {
                // 发送
                producer.send(new ProducerRecord<>(topic, "cur-time", String.format("id: %d, time : %d.", value, System.currentTimeMillis())), (metadata, exception) -> {

                });
            });
            // 最后记得刷新出去.
            producer.flush();
        }
    }
}
