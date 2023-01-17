import kafka_version.Transaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.HashMap;
import java.util.Properties;

public class validator3 {
    static HashMap<String, Long> bankBalance = new HashMap<String, Long>();

    public static void main(String[] args) {

        final String inputTopic = "bigTX";
        final String OutputTopic = "balance";

        // setting properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple_validator");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);


        //create TxSerde
        TxDeserializer<Transaction> TxDeserializer =
                new TxDeserializer<>();
        TxSerializer<Transaction> TxSerializer =
                new TxSerializer<>();
        Serde<Transaction> TxSerde =
                Serdes.serdeFrom(TxSerializer, TxDeserializer);

        bankBalance.put("100", 000000L);
        bankBalance.put("101", 000000L);
        bankBalance.put("102", 000000L);

        //build stream
        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Transaction> inputStream = builder.stream(
                inputTopic, Consumed.with(Serdes.String(), TxSerde));

        inputStream.mapValues((key, value) -> bankBalance.get(key) - value.getAmount())
                .peek((key, value) -> bankBalance.put(key, value))
                .to(OutputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        inputStream.map((key, value) -> new KeyValue<>(value.getOutBank(), bankBalance.get(value.getOutBank()) + value.getAmount()))
                .peek((key, value) -> bankBalance.put(key, value))
                .peek((key, value) -> System.out.println("bank 100: " + bankBalance.get("100") +
                        ",bank 101: " + bankBalance.get("101") + ",bank 102: " + bankBalance.get("102")))
                .to(OutputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build(config);
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, config);

        kafkaStreams.start();
    }
}



