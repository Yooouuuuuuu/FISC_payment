import kafka_version.Transaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.HashMap;
import java.util.Properties;

public class validator2 {
    static HashMap<String, Long> bankBalance = new HashMap<String, Long>();

    public static void main(String[] args) {

        final String inputTopic = "bigTX";
        final String OutputTopic = "balance";

        // setting properties
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple_validator");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //create TxSerde
        TxDeserializer<Transaction> TxDeserializer =
                new TxDeserializer<>();
        TxSerializer<Transaction> TxSerializer =
                new TxSerializer<>();
        Serde<Transaction> TxSerde =
                Serdes.serdeFrom(TxSerializer, TxDeserializer);

        bankBalance.put("100", 1000000L);
        bankBalance.put("101", 1000000L);
        bankBalance.put("102", 1000000L);

        //build stream
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Transaction> inputStream = streamsBuilder.stream(
                inputTopic, Consumed.with(Serdes.String(), TxSerde)
                //Materialized.<String, Transaction, KeyValueStore<Bytes, byte[]>>as("balanceStore" /* table/store name */)
                        //.withKeySerde(Serdes.String()) /* key serde */
                        //.withValueSerde(TxSerde) /* value serde */
        );


        KStream<String, Long> validatorIn =
                inputStream.mapValues((key, value) -> bankBalance.get(key) - value.getAmount());
        validatorIn
                .peek((key, value) -> bankBalance.put(key, value))
                .to(OutputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        /*
        KStream<String, Long> validatorOut =
                inputStream.map((key, value) -> new KeyValue<>(value.getOutBank(), bankBalance.get(key) + value.getAmount()));
        validatorOut
                .peek((key, value) -> bankBalance.put(key, value))
                .peek((key, value) -> System.out.println(key + ", " + value))
                .to(OutputTopic, Produced.with(Serdes.String(), Serdes.Long()));
*/


        KafkaStreams kafkaStreams =
                new KafkaStreams(streamsBuilder.build(), streamsConfig);
        kafkaStreams.start();

    }
}


[main] WARN org.apache.kafka.streams.internals.metrics.ClientMetrics - Error while loading kafka-streams-version.properties
        java.lang.NullPointerException: inStream parameter is null
        at java.base/java.util.Objects.requireNonNull(Objects.java:246)
        at java.base/java.util.Properties.load(Properties.java:406)
        at org.apache.kafka.streams.internals.metrics.ClientMetrics.<clinit>(ClientMetrics.java:53)
        at org.apache.kafka.streams.KafkaStreams.<init>(KafkaStreams.java:894)
        at org.apache.kafka.streams.KafkaStreams.<init>(KafkaStreams.java:856)
        at org.apache.kafka.streams.KafkaStreams.<init>(KafkaStreams.java:826)
        at org.apache.kafka.streams.KafkaStreams.<init>(KafkaStreams.java:738)
        at validator2.main(validator2.java:69)



