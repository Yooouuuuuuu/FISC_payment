import kafka_version.Transaction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;


import java.util.HashMap;
import java.util.Properties;

import static java.lang.System.getProperties;

public class validator {
    static HashMap<String, Long> bankBalance = new HashMap<String, Long>();

    public static void main(String[] args) {

        final String inputTopic = "bigTX";
        final String OutputTopic = "balance";
        Serde<String> stringSerde = Serdes.String();

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

        KTable<String, Transaction> inputStream = streamsBuilder.table(
                inputTopic, Consumed.with(Serdes.String(), TxSerde)
                //Materialized.<String, Transaction, KeyValueStore<Bytes, byte[]>>as("balanceStore" /* table/store name */)
                        //.withKeySerde(Serdes.String()) /* key serde */
                        //.withValueSerde(TxSerde) /* value serde */
        );


        KTable<String, Long> validatorIn =
                inputStream.mapValues((key, value) -> bankBalance.get(key) - value.getAmount());
        validatorIn.toStream()
                .peek((key, value) -> bankBalance.put(key, value))
                .to(OutputTopic, Produced.with(Serdes.String(), Serdes.Long()));

/*
        KStream<String, Long> validatorOut =
                inputStream.map((key, value) -> new KeyValue<> (value.getOutBank(), bankBalance.get(key) + value.getAmount()));
        validatorOut.to(OutputTopic, Produced.with(Serdes.String(), Serdes.Long()));
*/


        KafkaStreams kafkaStreams =
                new KafkaStreams(streamsBuilder.build(), streamsConfig);
        kafkaStreams.start();

    }
}