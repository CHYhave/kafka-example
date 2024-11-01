package com.xiaoyu.consumer;

import com.alibaba.fastjson2.JSONObject;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author chy
 * @description
 * @date 2024/10/29
 */
public class Consumer {
    public static void main(String[] args) {
        simpleStringConsumer();
    }

    // 按照规则，一个消费者使用一个线程。
    public static void simpleStringConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "124.220.233.169:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("customerCountries"));

        Duration timeout = Duration.ofMillis(100);

        Map<String, Integer> custCountryMap = new HashMap<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, " +
                                "customer = %s, country = %s\n",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), record.value());
                int updatedCount = 1;
                if (custCountryMap.containsKey(record.value())) {
                    updatedCount = custCountryMap.get(record.value()) + 1;
                }
                custCountryMap.put(record.value(), updatedCount);

                JSONObject json = new JSONObject(custCountryMap);
                System.out.println(json.toString());
            }
        }
    }

    // 方式1自动提交
    public static void autoCommitConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "124.220.233.169:9092");
        props.put("group.id", "CountryCounter");
        // 自动提交开关
        props.put("enable.auto.commit", "true");
        // 自动提交间隔
        props.put("auto.commit.interval.ms", "5");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("customerCountries"));

        Duration timeout = Duration.ofMillis(100);

        Map<String, Integer> custCountryMap = new HashMap<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, " +
                                "customer = %s, country = %s\n",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), record.value());

            }
        }
    }

    // 方式2同步提交
    public static void syncCommitConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "124.220.233.169:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("customerCountries"));

        Duration timeout = Duration.ofMillis(100);

        Map<String, Integer> custCountryMap = new HashMap<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, " +
                                "customer = %s, country = %s\n",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), record.value());

            }
            // 提交当前poll的最大偏移量
            consumer.commitSync();
        }
    }

    // 方式3异步提交
    public static void asyncCommitConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "124.220.233.169:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("customerCountries"));

        Duration timeout = Duration.ofMillis(100);
        AtomicInteger id = new AtomicInteger(0);

        Map<String, Integer> custCountryMap = new HashMap<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, " +
                                "customer = %s, country = %s\n",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), record.value());

            }
            // 提交当前poll的最大偏移量
            final int currentCommitId = id.getAndIncrement();
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (currentCommitId == id.get()) {
                        consumer.commitAsync(offsets, this);
                    }
                }
            });
        }
    }

    // 最佳实践：同步异步组合
    public static void asyncAndSyncCommitConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "124.220.233.169:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("customerCountries"));

        Duration timeout = Duration.ofMillis(100);
        AtomicInteger id = new AtomicInteger(0);
        boolean isClose = false;
        Map<String, Integer> custCountryMap = new HashMap<>();
        while (!isClose) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, " +
                                "customer = %s, country = %s\n",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), record.value());

            }
            // 提交当前poll的最大偏移量
            final int currentCommitId = id.getAndIncrement();
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (currentCommitId == id.get()) {
                        consumer.commitAsync(offsets, this);
                    }
                }
            });
        }
        try {
            consumer.commitSync();
        } catch (Exception e ) {
            e.printStackTrace();
        }
    }

    // 手动控制提交便宜量
    public static void manuelOffsetCommit() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "124.220.233.169:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("customerCountries"));

        Duration timeout = Duration.ofMillis(100);
        Map<TopicPartition, OffsetAndMetadata> currentOffsets =
                new HashMap<>();
        int count = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
                record.topic(), record.partition(), record.offset(),
                        record.key(), record.value());
                currentOffsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset()+1, "no metadata"));
                if (count % 1000 == 0) {
                    consumer.commitAsync(currentOffsets, null);
                }
                count++;
            }
        }
    }

    // 再均衡特殊处理
    public static void rebalanceConsumer() {

        Map<TopicPartition, OffsetAndMetadata> currentOffsets =
                new HashMap<>();
        Properties props = new Properties();
        props.put("bootstrap.servers", "124.220.233.169:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("customerCountries"), new ConsumerRebalanceListener() {

            // 消费者放弃分区所有权时调用到
            // 发生再均衡、消费者关闭
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // onPartitionsRevoked()会在进行正常的再均衡并且有消费者放弃分区所有权时被调用。如果它被调用，那么参数就不会是空集合。
                System.out.println("Lost partitions in rebalance. " +
                        "Committing current offsets:" + currentOffsets);
                consumer.commitSync(currentOffsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //onPartitionsAssigned()在每次进行再均衡时都会被调用，
                // 以此来告诉消费者发生了再均衡。如果没有新的分区分配给消费者，那么它的参数就是一个空集合
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                //  这个方法只会在使用了协作再均衡算法，并且之前不是通过再均衡获得的分区被重新分配给其他消费者时调用（原生的分区被其他分区获取，或者自己放弃）
                // （之前通过再均衡获得的分区被重新分配时会调用onPartitionsRevoked()）
                //onPartitionsRevoked()会在进行正常的再均衡并且有消费者放弃分区所有权时被调用。如果它被调用，那么参数就不会是空集合。
                ConsumerRebalanceListener.super.onPartitionsLost(partitions);
            }
        });

        Duration timeout = Duration.ofMillis(100);
        AtomicInteger id = new AtomicInteger(0);

        Map<String, Integer> custCountryMap = new HashMap<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, " +
                                "customer = %s, country = %s\n",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), record.value());

            }
            // 提交当前poll的最大偏移量
            final int currentCommitId = id.getAndIncrement();
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (currentCommitId == id.get()) {
                        consumer.commitAsync(offsets, this);
                    }
                }
            });
        }
    }



    public static void consumeFromSpecialOffset() {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets =
                new HashMap<>();
        Properties props = new Properties();
        props.put("bootstrap.servers", "124.220.233.169:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Long oneHourEarlier = Instant.now().atZone(ZoneId.systemDefault())
                .minusHours(1).toEpochSecond();
        Map<TopicPartition, Long> partitionTimestampMap = consumer.assignment()
                .stream()
                .collect(Collectors.toMap(tp -> tp, tp -> oneHourEarlier));
        Map<TopicPartition, OffsetAndTimestamp> offsetMap
                = consumer.offsetsForTimes(partitionTimestampMap);

        for(Map.Entry<TopicPartition,OffsetAndTimestamp> entry: offsetMap.entrySet()) {
            consumer.seek(entry.getKey(), entry.getValue().offset());
        }
    }

    public static void gracefulShutdown() {


        Map<TopicPartition, OffsetAndMetadata> currentOffsets =
                new HashMap<>();
        Properties props = new Properties();
        props.put("bootstrap.servers", "124.220.233.169:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Duration timeout = Duration.ofMillis(10000);
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Starting exit...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // 一直循环，直到按下Ctrl-C组合键，关闭钩子会在退出时做清理工作
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(timeout);
                System.out.println(System.currentTimeMillis() +
                        "-- waiting for data...");
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
                }
                for (TopicPartition tp: consumer.assignment())
                    System.out.println("Committing offset at position:" +
                            consumer.position(tp));
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            // 忽略异常 ➌
        } finally {
            consumer.close();
            System.out.println("Closed consumer and we are done");
        }
    }
}
