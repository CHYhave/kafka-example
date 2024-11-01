
package com.xiaoyu.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ConsumerSeek {

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

        long oneHourEarlier = Instant.now().atZone(ZoneId.systemDefault())
                .minusMinutes(15).toEpochSecond();
        Map<TopicPartition, Long> partitionTimestampMap = consumer.assignment()
                .stream()
                .collect(Collectors.toMap(tp -> tp, tp -> oneHourEarlier));
        Map<TopicPartition, OffsetAndTimestamp> offsetMap
                = consumer.offsetsForTimes(partitionTimestampMap);

        for(Map.Entry<TopicPartition,OffsetAndTimestamp> entry: offsetMap.entrySet()) {
            consumer.seek(entry.getKey(), entry.getValue().offset());
        }
    }
}
