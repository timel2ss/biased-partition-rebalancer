package client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.util.Properties;

public class Producer {
    private static final String TOPIC_A = "topic-A";
    private static final String TOPIC_B = "topic-B";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("acks", "all");
        configs.put("block.on.buffer.full", "true");
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");   // serialize 설정
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialize 설정

        // producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        // message 전달
        for (int i = 0; i < 300000; i++) {
            String v = "log-id:" + i + " " + LocalDateTime.now();
            producer.send(new ProducerRecord<>(TOPIC_A, v));
        }

        for (int i = 0; i < 30000; i++) {
            String v = String.valueOf(i);
            producer.send(new ProducerRecord<>(TOPIC_B, v));
        }

        // 종료
        producer.flush();
        producer.close();
    }
}