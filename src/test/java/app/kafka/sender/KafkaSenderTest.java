package app.kafka.sender;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class KafkaSenderTest {

    @Test
    @SneakyThrows
    void sendMessage() {
        KafkaSender kafkaSender = new KafkaSender();
        int size = 10000;
        for(int i = 0; i<size; i++) {
            Future<RecordMetadata> future = kafkaSender.sendMessage("transaction1", "{\"item\":\"book6\", \"price\":1020.99}");
            RecordMetadata recordMetadata = future.get();
            assertNotNull(recordMetadata);
            log.info("recordMetadata::"+recordMetadata);
        }

    }
}