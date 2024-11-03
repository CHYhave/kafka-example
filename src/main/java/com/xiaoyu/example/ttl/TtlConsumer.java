package com.xiaoyu.example.ttl;

import com.xiaoyu.Constant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author chy
 * @description
 * @date 2024/11/1
 */
public class TtlConsumer {
    public static void main(String[] args) {

        HashMap<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, TtlConstant.CONSUME_GROUP_ID);
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BROKER_URL);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList(TtlConstant.TTL_TOPIC));
            while (true) {
                ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
                poll.forEach(record ->
                        System.out.println(String.format("topic: %s, key: %s, value: %s, offset:%d, partition: %s",
                                record.topic(), record.key(), record.value(), record.offset(), record.partition())));
                // 提交偏移量.
                consumer.commitSync();
            }
        }
    }
}
