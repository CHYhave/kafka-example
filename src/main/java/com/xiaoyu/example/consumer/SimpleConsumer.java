package com.xiaoyu.example.consumer;

import com.xiaoyu.Constant;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @author chy
 * @description
 * @date 2024/11/1
 */
public class SimpleConsumer {
    private static final String TOPIC = "simple";
    private static final String GROUP_ID = "SIMPLE_GROUP";

    public static void main(String[] args) {

        HashMap<String, Object> config = getConfig();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (TopicPartition tp : records.partitions()) {
                    for (ConsumerRecord<String, String> record : records.records(tp)) {
                        System.out.println(record.partition() + ":" + record.value());
                    }
                }

                consumer.commitSync();
            }
        }
    }

    public static void rebalanceConsumer() {

        HashMap<String, Object> config = getConfig();
        HashMap<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                    consumer.commitSync(currentOffsets);
                    currentOffsets.clear();
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    for (TopicPartition partition : partitions) {
                        // 可以通过该回调，在rebalance时恢复消费进度
//                        consumer.seek(partition, getOffSetFromDB(partition));
                    }
                }
            });

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                }
                consumer.commitSync(currentOffsets, null);
            }
        }
    }


    public static void seek1() {
        HashMap<String, Object> config = getConfig();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            // 调用seek前必须poll一次，为消费者分配分区
            // 注意poll的时长设置，如果设置为0直接返回，seek将无法生效
            consumer.poll(Duration.ofMillis(10000));
            Set<TopicPartition> assignment = consumer.assignment();
            for (TopicPartition tp : assignment) {
                consumer.seek(tp, 10);
            }
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.partition() + ":" + record.value());
                }
            }
        }
    }

    public static void seek2() {
        HashMap<String, Object> config = getConfig();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            Set<TopicPartition> assignment = new HashSet<TopicPartition>();
            // 确保poll成功
            while (assignment.size() == 0) {
                consumer.poll(Duration.ofMillis(10000));
                assignment = consumer.assignment();
            }
            for (TopicPartition tp : assignment) {
                consumer.seek(tp, 10);
            }
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.partition() + ":" + record.value());
                }
            }
        }
    }

    public static void seekDemoConsumerLastDayMsg() {
        HashMap<String, Object> config = getConfig();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            Set<TopicPartition> assignment = new HashSet<TopicPartition>();
            // 确保poll成功
            while (assignment.size() == 0) {
                consumer.poll(Duration.ofMillis(10000));
                assignment = consumer.assignment();
            }
            HashMap<TopicPartition, Long> timestampToSearch = new HashMap<>();
            for (TopicPartition tp : assignment) {
                timestampToSearch.put(tp,
                        System.currentTimeMillis() - 24 * 3600 * 1000);
            }
            Map<TopicPartition, OffsetAndTimestamp> offsets =
                    consumer.offsetsForTimes(timestampToSearch);
            for (TopicPartition topicPartition : assignment) {
                OffsetAndTimestamp offsetAndTimestamp = offsets.get(topicPartition);
                if (offsetAndTimestamp != null) {
                    consumer.seek(topicPartition, offsetAndTimestamp.offset());
                }
            }
        }
    }

    private static HashMap<String, Object> getConfig() {
        HashMap<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BROKER_URL);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return config;
    }
}
