package com.xiaoyu.example.multithread;

import com.xiaoyu.Constant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created By Have
 * 2024/11/3 23:01
 */
public class MultiThreadConsumer {
    private static final String GROUP_ID = "MULTI_THREAD";
    private static final String TOPIC = "multi-thread";

    public static void main(String[] args) {
        Map<String, Object> config = getConfig();
        new KafkaConsumerThread(config, TOPIC, Runtime.getRuntime().availableProcessors());
    }

    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> consumer;
        private ExecutorService executorService;
        private int threadNumber;

        public KafkaConsumerThread(Map<String, Object> config, String topic, int num) {
            consumer = new KafkaConsumer<>(config);
            executorService = new ThreadPoolExecutor(threadNumber, threadNumber, 0L,
                    TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            threadNumber = num;
            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        executorService.submit(new RecorderHandler(records));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }
    }

    public static class RecorderHandler extends Thread {
        private ConsumerRecords<String, String> records;

        public RecorderHandler(ConsumerRecords<String, String> records) {
            this.records = records;
        }

        @Override
        public void run() {
            // processing recorders
        }
    }

    private static Map<String, Object> getConfig() {
        HashMap<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BROKER_URL);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return config;
    }
}
