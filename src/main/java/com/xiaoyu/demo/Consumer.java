package com.xiaoyu.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;

public class Consumer {

    public static void main(String[] args) {

        final String topic = "topic-1";
        final String group = "consumer-1";
        final String url = "124.220.233.169:9092";

        HashMap<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(500);
            poll.forEach(record ->
                    System.out.println(String.format("topic: %s, key: %s, value: %s, offset:%d.",
                            record.topic(), record.key(), record.value(), record.offset())));
            // 提交偏移量.
            consumer.commitSync();
        }
    }
}
