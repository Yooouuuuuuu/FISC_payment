import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class consumeFromBalance {

    public static void main(String[] args) throws Exception {
        Properties props_consumer_String = new Properties();
        props_consumer_String.put("bootstrap.servers", "localhost:9092");
        props_consumer_String.put("group.id", "banks");
        props_consumer_String.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props_consumer_String.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props_consumer_String.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "off"); //"off", "trace", "debug", "info", "warn", "error".

        String input_topic = "balance";
        KafkaConsumer<String, Long> consumer_from_tmp =
                new KafkaConsumer<String, Long>(props_consumer_String);
        consumer_from_tmp.subscribe(Collections.singletonList(input_topic));

        Logger logger = LoggerFactory.getLogger(consumeFromBalance.class);

        while (true){
            ConsumerRecords<String, Long> records = consumer_from_tmp.poll(Duration.ofMillis(10));
            for(ConsumerRecord<String, Long> record : records) {
                logger.info("offset: " + record.offset() + ", Key: " + record.key() + ", Value: " + record.value() + ", Partition: " + record.partition());
            }
        }
    }

}

