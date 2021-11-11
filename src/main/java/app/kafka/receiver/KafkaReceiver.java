package app.kafka.receiver;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;

public class KafkaReceiver {
    private static final Logger logger = Logger.getLogger(KafkaReceiver.class);

    private volatile boolean doneConsuming = false;
    private int numberPartitions;
    private ExecutorService executorService;

    public KafkaReceiver(int numberPartitions) {
        this.numberPartitions = numberPartitions;
        executorService = Executors.newFixedThreadPool(numberPartitions);
    }

    public void startConsuming() {
        logger.info("start consuming messages");
        Properties properties = getConsumerProps();

        for (int i = 0; i < numberPartitions; i++) {
            Runnable consumerThread = getConsumerThread(properties);
            executorService.submit(consumerThread);
        }
    }

    private Runnable getConsumerThread(Properties properties) {
        return () -> {
            Consumer<String, String> consumer = null;
            try {
                consumer = new KafkaConsumer<>(properties);
                consumer.subscribe(Collections.singletonList("transactions"));
                while (!doneConsuming) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.of(5000, ChronoUnit.MILLIS));
                    for (ConsumerRecord<String, String> record : records) {
                        String message = String.format("Consumed: key = %s value = %s with offset = %d partition = %d",
                            record.key(), record.value(), record.offset(), record.partition());
                        System.out.println(message);
                    }
                    sleep(new Random().nextInt(5000));
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        };
    }

    private Properties getConsumerProps() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "zmart-purchases");
        properties.put("auto.offset.reset", "earliest");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "3000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return properties;
    }

    public static void main(String[] args) {
        KafkaReceiver kafkaReceiver = new KafkaReceiver(3);
        kafkaReceiver.startConsuming();

    }
}
