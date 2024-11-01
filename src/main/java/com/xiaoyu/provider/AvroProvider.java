package com.xiaoyu.provider;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author chy
 * @description
 * @date 2024/10/29
 */
public class AvroProvider {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        // avro需要一个独特的服务来储存序列化schema
//        props.put("schema.registry.url", schemaUrl);

        String topic = "customerContacts";

        Producer<String, Customer> producer = new KafkaProducer<>(props);

        // 不断生成新事件，直到有人按下Ctrl-C组合键
        while (true) {
            Customer customer = CustomerGenerator.getNext();
            System.out.println("Generated customer " +
                    customer.toString());
            ProducerRecord<String, Customer> record =
                    new ProducerRecord<>(topic, customer.getName(), customer);
            producer.send(record);
        }
    }
}
