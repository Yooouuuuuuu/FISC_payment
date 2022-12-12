import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class validatorForceAbort {
    static HashMap<String, Long> bankBalance = new HashMap<String, Long>();
    static KafkaConsumer<String, Transaction> consumerFromBig;
    static KafkaConsumer<String, Transaction> consumerFromLocalBalance;
    static Producer<String, Transaction> producer;

    public static void main(String[] args) throws Exception {

        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "off"); //"off", "trace", "debug", "info", "warn", "error".
        InitConsumer();
        InitProducer();
        Logger logger = LoggerFactory.getLogger(validatorForceAbort.class);
        producer.initTransactions();
        Boolean settingIncomplete = true;

        //poll from bigTX
        while (true) {
            ConsumerRecords<String, Transaction> records = consumerFromBig.poll(Duration.ofMillis(100));



            for (ConsumerRecord<String, Transaction> record : records) {
                logger.info("InBank: " + record.value().getInBank() + " ,OutBank: " + record.value().getOutBank() + " ,Value: " + record.value().getAmount() + " ,Offset:" + record.offset());

                producer.beginTransaction();        //start atomically transaction
                try {
                    //no.30 fault
                    if (record.value().getCategory() == 0) {
                        ProcessBig(record.value());
                    } else if (record.value().getCategory() == 1) {
                        ProcessAggregated(record.value());
                    } else if (record.value().getCategory() == 2) {
                        ProcessCompensate(record.value());
                    } else if (record.value().getCategory() == 3) {
                        InitBank(record.value());
                    }
                    consumerFromBig.commitSync();
                    producer.commitTransaction();
                    System.out.println("InBank: " + record.value().getInBank() + " , OutBank: " + record.value().getOutBank()
                            + " , Value: " + record.value().getAmount() + " , Offset:" + record.offset() + " , SerialNumber:" + record.value().getSerialNumber());
                    //System.out.println("committed");
            } catch ( Exception e ) {
                    producer.abortTransaction();    //end atomically transaction
                    System.out.println("Kafka transaction has been aborted.");
                    return;
                }
            }
        }
    }

    private static void InitConsumer() {
        //consumer consume from big
        Properties propsConsumerTx = new Properties();
        propsConsumerTx.put("bootstrap.servers", "localhost:9092");
        propsConsumerTx.put("group.id", "validator-main-group");
        propsConsumerTx.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        propsConsumerTx.put("value.deserializer", "TxDeserializer");
        propsConsumerTx.put("isolation.level", "read_committed");
        propsConsumerTx.put("enable.auto.commit", "false");
        propsConsumerTx.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        String input_topic = "bigTX";
        consumerFromBig =
                new KafkaConsumer<String, Transaction>(propsConsumerTx);
        consumerFromBig.subscribe(Collections.singletonList(input_topic));

        //consumer consume from localBalance
        Properties propsConsumerLocalBalance = new Properties();
        propsConsumerLocalBalance.put("bootstrap.servers", "localhost:9092");
        propsConsumerLocalBalance.put("group.id", "localBalance-group");
        propsConsumerLocalBalance.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        propsConsumerLocalBalance.put("value.deserializer", "TxDeserializer");
        propsConsumerLocalBalance.put("isolation.level", "read_committed");
        propsConsumerLocalBalance.put("enable.auto.commit", "false");
        propsConsumerLocalBalance.put("fetch.max.bytes", 0);
        consumerFromLocalBalance =
                new KafkaConsumer<String, Transaction>(propsConsumerLocalBalance);
        //seek(ToEnd) to topicPartition later, thus no subscribe.
    }

    private static void InitProducer() {
        Properties propsTxWrite = new Properties();
        propsTxWrite.put("bootstrap.servers", "localhost:9092");
        propsTxWrite.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        propsTxWrite.put("value.serializer", "TxSerializer");
        propsTxWrite.put("transactional.id", UUID.randomUUID().toString()); //Should be different between validators to avoid being fenced due to same transactional.id.
        propsTxWrite.put("enable.idempotence", "true");
        propsTxWrite.put("max.block.ms", "5000");
        producer = new KafkaProducer<>(propsTxWrite);
    }

    private static Transaction CompensationRecord(Transaction tx) {
        return new Transaction(tx.getOutBank(), "000", tx.getAmount(), -1, tx.getOutBankPartition(), -1, 2);
    }

    private static Transaction BalanceRecord(Transaction tx) {
        return new Transaction(tx.getInBank(), "000", bankBalance.get(tx.getInBank()), -1, -1, -1, -1);
    }

    private static void PollFromTmp(Transaction tx) {
        TopicPartition topicPartition = new TopicPartition("localBalance", tx.getInBankPartition());
        consumerFromLocalBalance.assign(List.of(topicPartition));
        consumerFromLocalBalance.seekToEnd(Collections.singleton(topicPartition));
        long latestOffset = consumerFromLocalBalance.position(topicPartition);
        boolean findingLast = true;
        while (findingLast) {
            consumerFromLocalBalance.seek(topicPartition, latestOffset);
            latestOffset -= 1;
            //System.out.println(consumerFromLocalBalance.position(topicPartition));

            ConsumerRecords<String, Transaction> balanceRecords = consumerFromLocalBalance.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Transaction> balanceRecord : balanceRecords) {
                bankBalance.compute(tx.getInBank(), (key, value) -> balanceRecord.value().getAmount());
                //System.out.println(tx.getInBank() + " now have " + bankBalance.get(tx.getInBank()));
                findingLast = false;
            }
        }
    }

    private static void ProcessBig(Transaction tx) throws ExecutionException, IOException, InterruptedException {
        PollFromTmp(tx);
        if (bankBalance.get(tx.getInBank()) >= tx.getAmount()) {
            bankBalance.compute(tx.getInBank(), (key, value) -> value - tx.getAmount());
            producer.send(new ProducerRecord<String, Transaction>("bigTX", tx.getOutBankPartition(), tx.getOutBank(), CompensationRecord(tx))).get();
            if (tx.getSerialNumber() == 35L) {
                producer.send(new ProducerRecord<String, Transaction>("successful", 100, tx.getInBank(), tx)).get(); //causes exception for testing
            }
            producer.send(new ProducerRecord<String, Transaction>("successfulTX", tx.getInBank(), tx)).get();
            producer.send(new ProducerRecord<String, Transaction>("localBalance", tx.getInBankPartition(), tx.getInBank(), BalanceRecord(tx))).get();
        } else {
            producer.send(new ProducerRecord<String, Transaction>("rejectedTX", tx.getInBank(), tx)).get();
            if (tx.getSerialNumber() == 35L) {
                producer.send(new ProducerRecord<String, Transaction>("successful", 100, tx.getInBank(), tx)).get(); //causes exception for testing
            }
            System.out.println("Big transaction cancelled.");
        }
    }

    private static void ProcessAggregated(Transaction tx) throws ExecutionException, InterruptedException {
        PollFromTmp(tx);
        if (bankBalance.get(tx.getInBank()) >= tx.getAmount()) {
            bankBalance.compute(tx.getInBank(), (key, value) -> value - tx.getAmount());
            producer.send(new ProducerRecord<String, Transaction>("localBalance", tx.getInBankPartition(), tx.getInBank(), BalanceRecord(tx))).get();
            producer.send(new ProducerRecord<String, Transaction>("bigTX", tx.getOutBankPartition(), tx.getOutBank(), CompensationRecord(tx))).get();
            producer.send(new ProducerRecord<String, Transaction>("successfulTX", tx.getInBank(), tx)).get();
        } else {
            producer.send(new ProducerRecord<String, Transaction>("bigTX", tx.getInBankPartition(), tx.getInBank(), tx)).get();
            System.out.println("Transaction suspended. Sent back to big topic.");
        }
    }

    private static void ProcessCompensate(Transaction tx) throws ExecutionException, InterruptedException {
        PollFromTmp(tx);
        bankBalance.compute(tx.getInBank(), (key, value) -> value + tx.getAmount());
        producer.send(new ProducerRecord<String, Transaction>("localBalance", tx.getInBankPartition(), tx.getInBank(), BalanceRecord(tx))).get();
    }

    private static void InitBank(Transaction tx) throws ExecutionException, InterruptedException {
        bankBalance.put(tx.getInBank(), tx.getAmount());
        producer.send(new ProducerRecord<String, Transaction>("localBalance", tx.getInBankPartition(), tx.getInBank(), BalanceRecord(tx))).get();
        producer.send(new ProducerRecord<String, Transaction>("successfulTX", tx.getInBank(), tx)).get();
        System.out.println("Bank " + tx.getInBank() + " has been initialized to balance " + tx.getAmount() + ".");
    }
}
