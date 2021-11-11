package app.kafka.sender;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
public class KafkaSender {
    Properties properties = new Properties();
    KafkaProducer<String, String> producer = null;

    public KafkaSender() {
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("retries", "3");
        properties.put("compression.type", "snappy");
//        properties.put("partitioner.class",PurchaseKeyPartitioner.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public Future<RecordMetadata> sendMessage(String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>("transactions", key, message);

        Callback callback = ((metadata, exception) -> {
            if (exception != null) {
                log.error("error occurred " + exception);
            }
        });

        return producer.send(record, callback);
    }
}
