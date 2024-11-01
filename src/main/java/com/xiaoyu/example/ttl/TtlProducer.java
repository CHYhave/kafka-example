package com.xiaoyu.example.ttl;

import com.xiaoyu.ByteUtils;
import com.xiaoyu.ProducerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import static com.xiaoyu.example.ttl.TtlConstant.topic;
import static com.xiaoyu.example.ttl.TtlConstant.url;

public class TtlProducer {

    static KafkaProducer<String, String> producer = ProducerFactory.getStringProducer(url);


    public static void main(String[] args) throws InterruptedException {
        // 配置.
        send(0,5);
        send(11 * 1000,10);
        send(0,20);
        // 最后记得刷新出去.
        producer.flush();
    }

    public static void send(long aheadTime, int timeout) {
        producer.send(
                new ProducerRecord<>(
                        topic, 0, System.currentTimeMillis(),
                        "cur-time",
                        String.format("id: %d, time : %d.", 1, (System.currentTimeMillis() - aheadTime)),
                        new RecordHeaders().add(new RecordHeader("ttl", ByteUtils.longToBytes(timeout)))
                ),
                (metadata, exception) -> {}
        );
    }
}
