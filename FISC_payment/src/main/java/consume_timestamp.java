import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.*;

public class consume_timestamp {
    public static void main(String[] args) throws Exception {

        int numOfTX = Integer.parseInt(args[0]); //change input here

        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "off"); //"off", "trace", "debug", "info", "warn", "error".
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", UUID.randomUUID().toString()); //if read without init, same group is an issue.
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "TxDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, Transaction> consumerFromBig =
                new KafkaConsumer<String, Transaction>(props);
        KafkaConsumer<String, Transaction> consumerFromSuc =
                new KafkaConsumer<String, Transaction>(props);
        KafkaConsumer<String, Transaction> consumerFromBal =
                new KafkaConsumer<String, Transaction>(props);

        String input_topic = "bigTX";
        consumerFromBig.subscribe(Collections.singletonList(input_topic));
        consumeAndWrite("/home/yooouuuuuuu/kafka_projects/TXtimestamps/bigTX.txt", consumerFromBig, numOfTX);
        //consumeAndWrite("src/main/java/timestamp/bigTX.txt", consumerFromBig, numOfTX);
        consumerFromBig.close();

        input_topic = "balance";
        consumerFromBal.subscribe(Collections.singletonList(input_topic));
        consumeAndWrite("/home/yooouuuuuuu/kafka_projects/TXtimestamps/balance.txt", consumerFromBal, numOfTX);
        //consumeAndWrite("src/main/java/timestamp/balance.txt", consumerFromBal, numOfTX);
        consumerFromBal.close();
    }

    private static void consumeAndWrite(String filename, KafkaConsumer consumer, int numOfTX) throws FileNotFoundException {

        long serialNumber;
        PrintWriter writer = new PrintWriter(filename);
        long seconds = System.currentTimeMillis();
        while (seconds + (30 * 1000) > System.currentTimeMillis()) { //might have to set bigger if input increase
            ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Transaction> record : records){
                if (record.value().getSerialNumber() == -1) {
                    serialNumber = numOfTX; //change along with input
                }else {
                    serialNumber = record.value().getSerialNumber();
                }
                //System.out.println("Now writing...");
                writer.println(serialNumber);
                writer.println(record.timestamp());
            }
        }
        writer.close();
        System.out.println(filename + " is written complete.");
    }
}
