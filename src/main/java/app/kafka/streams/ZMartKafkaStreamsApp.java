package app.kafka.streams;

import app.kafka.clients.producer.MockDataProducer;
import app.kafka.model.Purchase;
import app.kafka.model.PurchasePattern;
import app.kafka.model.RewardAccumulator;
import app.kafka.util.serde.StreamsSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

@Slf4j
public class ZMartKafkaStreamsApp {

    public static void main(String[] args) throws InterruptedException {
        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Purchase> purchaseKStream = streamsBuilder
            .stream("transactions", Consumed.with(stringSerde, purchaseSerde))
            .peek((k,p)->log.info("Purchase::::"+p.toString()))
            .filter((k,p)->p.getCreditCardNumber() != null)
            .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        KStream<String, PurchasePattern> patternKStream = purchaseKStream.
            mapValues(purchase -> PurchasePattern.builder(purchase).build());

        patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
        patternKStream.to("patterns", Produced.with(stringSerde,purchasePatternSerde));


        KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream.
            mapValues(purchase -> RewardAccumulator.builder(purchase).build());
        rewardsKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards"));
        rewardsKStream.to("rewards1", Produced.with(stringSerde,rewardAccumulatorSerde));

        purchaseKStream.print(Printed.<String, Purchase>toSysOut().withLabel("purchases"));
        purchaseKStream.to("purchases1", Produced.with(stringSerde,purchaseSerde));


        // used only to produce data for this application, not typical usage
        MockDataProducer.producePurchaseData();

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),streamsConfig);
        log.info("ZMart First Kafka Streams Application Started");
        kafkaStreams.start();
        Thread.sleep(65000);
        log.info("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
