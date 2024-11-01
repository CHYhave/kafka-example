package com.xiaoyu.provider;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author chy
 * @description
 * @date 2024/10/29
 */
public class Provider {
    public static void main(String[] args) {
        sendAndForgetSend();
        syncSend();
    }

    public static KafkaProducer<String, String> simpleStringProvider() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "124.220.233.169:9092");
        kafkaProps.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        // 生产者标识，最好唯一，方便排查问题
        kafkaProps.put("client.id", "simpleStringProvider");
        // 生产者到broker发送标识
        // 0 标识生产者在发送消息后不用确认ack，就当提交了，消息丢失了也不知道
        // 1 只接受首领副本的ack，如果失败了，broker会返回一个错误异常
        // all 只有全部的副本都ack了，才算提交成功
        kafkaProps.put("acks", "1");

        // 生产者调用send最大耗时
        // 一般这里超时可能是生产缓冲队列满了，或者gc？
        kafkaProps.put("max.block.ms", "3");
        // 生产者调用send提交到缓冲队列后，到客户端放弃发送的超时时间
        // 注意生产者在发送失败后会不断重试，在该超时时间到达前会持续重试
        kafkaProps.put("delivery.timeout.ms", "120");
        // 生产者等待broker响应超时时间，触发后会重试，或者执行异常回调函数
        kafkaProps.put("request.timeout.ms", "3");

        // 生产者最大重试次数，在配置delivery.timeout.ms后，该值可以配置大一些
        kafkaProps.put("retries", "3");
        // 重试间隔
        kafkaProps.put("retry.backoff.ms", "3");

        // 发送间隔，批量发送的优化
        kafkaProps.put("linger.ms", "10");
        // 批大小，单位是字节, 一个分区的才能被放入一个批次
        kafkaProps.put("batch.size", "65535");
        // 在收到broker消息前，可以发送最多多少个消息批次，建议值是2，默认是5
        kafkaProps.put("max.in.flight.requests.per.connection", "2");

        // 消息缓冲区大小
        kafkaProps.put("buffer.memory", "1024");
        // 单个请求的大小，比如配置了1024kb，那么单个请求最多支持发送1024个1kb的消息，单个消息的大小也不能超过1024KB
        kafkaProps.put("max.request.size", "1024");

        // 压缩算法 snappy、gzip、lz4或zstd
        kafkaProps.put("compression.type", "snappy");

        // 幂等性控制，每条消息将加上一个序列号
        // 如果要启用幂等性，那么max.in.flight.requests.per.connection应小于或等于5、retries应大于0，并且acks被设置为all。如果设置了不恰当的值，则会抛出ConfigException异常。
        kafkaProps.put("enable.idempotenc", "true");

        return new KafkaProducer<>(kafkaProps);
    }


    // 第一种发送方式： 发送并忘记 类似于OneWay？
    public static void sendAndForgetSend() {
        KafkaProducer<String, String> producer = simpleStringProvider();

        ProducerRecord<String, String> record =
                new ProducerRecord<>("CustomerCountry", "Precision Products",
                        "France");
        try {
            // 不是真正的发送，消息会被放到一个缓冲队列
            // 返回一个future对象
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 方式2: 同步发送
    public static void syncSend() {
        KafkaProducer<String, String> producer = simpleStringProvider();
        ProducerRecord<String, String> record =
                new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
        try {
            producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                // 可以实现补偿，或者回滚？
                e.printStackTrace();
            }
        }
    }

    // 方式3: 异步发送
    // 注意回调函数会在生产者主线程执行，如果回调执行时间很长会阻塞主线程
    // provider的线程模型，主线程负责发起调用，其他线程负责如发送消息、定时任务等其他任务
    public static void asyncSend() {
        KafkaProducer<String, String> producer = simpleStringProvider();
        ProducerRecord<String, String> record =
                new ProducerRecord<>("CustomerCountry", "Biomedical Materials", "USA");
        producer.send(record, new DemoProducerCallback());
    }

    public static void recordHeader() {
        KafkaProducer<String, String> producer = simpleStringProvider();
        ProducerRecord<String, String> record =
                new ProducerRecord<>("CustomerCountry", "Biomedical Materials", "USA");
        record.headers().add("privacy-level", "YOLO".getBytes(StandardCharsets.UTF_8));
        producer.send(record, new DemoProducerCallback());
    }
}
