package app.kafka.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

@Slf4j
public class KafkaConsumerMonitor {

    private final String monitoringConsumerGroupID = "monitoring_consumer_" + UUID.randomUUID();

    KafkaConsumer consumer;

    KafkaConsumer<?, ?> endOffsetConsumer;

    public Map<TopicPartition, PartionOffsets> getConsumerGroupOffsets(String host, String topic, String groupId) {
        Map<TopicPartition, Long> logEndOffset = getLogEndOffset(topic, host);
        if (consumer == null)
            consumer = createNewConsumer(groupId, host);

        BinaryOperator<PartionOffsets> mergeFunction = (a, b) -> {
            throw new IllegalStateException();
        };
        Map<TopicPartition, PartionOffsets> result = logEndOffset.entrySet()
            .stream()
            .collect(Collectors.toMap(
                entry -> (entry.getKey()),
                entry -> {
                    Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Set.of(entry.getKey()));
                    return new PartionOffsets(entry.getValue(), committed.get(entry.getKey()).offset(), entry.getKey().partition(), topic);
                }, mergeFunction));


        return result;
    }

    public Map<TopicPartition, Long> getLogEndOffset(String topic, String host) {
        Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();
        if (endOffsetConsumer == null)
            endOffsetConsumer = createNewConsumer(monitoringConsumerGroupID, host);
        List<PartitionInfo> partitionInfoList = endOffsetConsumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = partitionInfoList.stream().map(pi -> new TopicPartition(topic, pi.partition())).collect(Collectors.toList());
        endOffsetConsumer.assign(topicPartitions);
        endOffsetConsumer.seekToEnd(topicPartitions);
        topicPartitions.forEach(topicPartition -> endOffsets.put(topicPartition, endOffsetConsumer.position(topicPartition)));
//        endOffsetConsumer.close();
        return endOffsets;
    }

    private static KafkaConsumer<?, ?> createNewConsumer(String groupId, String host) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    public static void main(String[] args) {
        KafkaConsumerMonitor kafkaConsumerMonitor = new KafkaConsumerMonitor();
        while (true) {
            Map<TopicPartition, PartionOffsets> transactions = kafkaConsumerMonitor.getConsumerGroupOffsets("localhost:9092", "transactions", "zmart-purchases");
            log.info("transactions::" + transactions);
            sleep(5000);
        }
    }

    @SneakyThrows
    private static void sleep(int mills) {
        TimeUnit.MILLISECONDS.sleep(mills);
    }

    @AllArgsConstructor
    @ToString
    @Data
    public static class PartionOffsets {
        private long endOffset;
        private long currentOffset;
        private int partition;
        private String topic;
    }
}
