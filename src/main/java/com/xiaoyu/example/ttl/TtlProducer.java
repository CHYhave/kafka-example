package com.xiaoyu.example.ttl;

import com.xiaoyu.ByteUtils;
import com.xiaoyu.ProducerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import static com.xiaoyu.Constant.BROKER_URL;

public class TtlProducer {

    static KafkaProducer<String, String> producer = ProducerFactory.getStringProducer(BROKER_URL);


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
                        TtlConstant.TTL_TOPIC,
                        0,
                        System.currentTimeMillis() - aheadTime,
                        null,
                        "msg_ttl",
                        new RecordHeaders().add(
                                new RecordHeader(
                                        "ttl",
                                        ByteUtils.longToBytes(timeout)
                                )
                        )
                ),
                (metadata, exception) -> {}
        );
    }
}
