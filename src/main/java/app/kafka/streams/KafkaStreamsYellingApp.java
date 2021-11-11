package app.kafka.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

@Slf4j
public class KafkaStreamsYellingApp {

    public static void main(String[] args) throws InterruptedException {
        new KafkaStreamsYellingApp().start();
    }

    KafkaStreams kafkaStreams = null;

    public void start() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling-app-id");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig config = new StreamsConfig(properties);
        Serde<String> serde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> firstStream = builder.stream("src-topic", Consumed.with(serde, serde));
        KStream<String, String> upperCasedStream = firstStream.mapValues(s -> s.toUpperCase());
        upperCasedStream.to("out-topic", Produced.with(serde, serde));

        kafkaStreams = new KafkaStreams(builder.build(), config);

        kafkaStreams.start();
    }

    public void shutdown() {
        log.info("Shutting down the Yelling APP now");
        kafkaStreams.close();
    }
}
