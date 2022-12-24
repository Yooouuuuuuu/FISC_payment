package old;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class validator_maxPoll {
    static HashMap<String, Long> bankBalance = new HashMap<String, Long>();
    static KafkaConsumer<String, Transaction> consumerFromBig;
    static KafkaConsumer<String, Transaction> consumerFromLocalBalance;
    static Producer<String, Transaction> producer;

    public static void main(String[] args) throws Exception {

        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "off"); //"off", "trace", "debug", "info", "warn", "error".
        InitConsumer(Integer.parseInt(args[0]));
        InitProducer();
        Logger logger = LoggerFactory.getLogger(validator_maxPoll.class);
        //System.out.println(Integer.parseInt(args[0]));
        producer.initTransactions();

        //poll from bigTX
        while (true) {
            ConsumerRecords<String, Transaction> records = consumerFromBig.poll(Duration.ofMillis(100));
            producer.beginTransaction();        //start atomically transaction
            try {
                for (ConsumerRecord<String, Transaction> record : records) {
                    logger.info("InBank: " + record.value().getInBank() + " ,OutBank: " + record.value().getOutBank() + " ,Value: " + record.value().getAmount() + " ,Offset:" + record.offset());
                    if (record.value().getCategory() == 0) {
                        ProcessBig(record.value());
                    } else if (record.value().getCategory() == 1) {
                        ProcessAggregated(record.value());
                    } else if (record.value().getCategory() == 2) {
                        ProcessCompensate(record.value());
                    } else if (record.value().getCategory() == 3) {
                        InitBank(record.value());
                    }
                }
                consumerFromBig.commitSync();
                producer.commitTransaction();
                //System.out.println("one poll finish with " + records.count() + " records");

            } catch ( Exception e ) {
                producer.abortTransaction();//end atomically transaction
                bankBalance = new HashMap<String, Long>();
                System.out.println("Tx aborted, bankBalance been reset.");
                //return;
            }
        }
    }

    private static void InitConsumer(int args) {
        //consumer consume from big
        Properties propsConsumerTx = new Properties();
        propsConsumerTx.put("bootstrap.servers", "localhost:9092");
        propsConsumerTx.put("group.id", "validator-main-group");
        propsConsumerTx.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        propsConsumerTx.put("value.deserializer", "TxDeserializer");
        propsConsumerTx.put("isolation.level", "read_committed");
        propsConsumerTx.put("enable.auto.commit", "false");
        propsConsumerTx.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        propsConsumerTx.put("max.poll.records", args);

        String input_topic = "bigTX";
        consumerFromBig =
                new KafkaConsumer<String, Transaction>(propsConsumerTx);
        consumerFromBig.subscribe(Collections.singletonList(input_topic),
                new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        //System.out.println("onPartitionsRevoked");
                    }
                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        System.out.println("bankBalance before rebalance: " + bankBalance);
                        bankBalance = new HashMap<String, Long>();
                    }});

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
        propsTxWrite.put("max.block.ms", "1000");
        producer = new KafkaProducer<>(propsTxWrite);
    }

    private static Transaction CompensationRecord(Transaction tx) {
        return new Transaction(tx.getOutBank(), tx.getInBank(), tx.getAmount(), tx.getSerialNumber(), tx.getOutBankPartition(), -1, 2);
    }

    private static Transaction BalanceRecord(Transaction tx) {
        return new Transaction(tx.getInBank(), "000", bankBalance.get(tx.getInBank()), -1, -1, -1, -1);
    }

    private static void PollFromTmp(Transaction tx) {
        if (!bankBalance.containsKey(tx.getInBank()) || Objects.equals(tx.getInBank(), "000")) {  // If tx.getInBank() =="000", Tx aborted, rollback to previous local balance.
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
    }

    private static void ProcessBig(Transaction tx) throws ExecutionException, IOException, InterruptedException {
        PollFromTmp(tx);
        if (bankBalance.get(tx.getInBank()) >= tx.getAmount()) {
            bankBalance.compute(tx.getInBank(), (key, value) -> value - tx.getAmount());
            producer.send(new ProducerRecord<String, Transaction>("bigTX", tx.getOutBankPartition(), tx.getOutBank(), CompensationRecord(tx)));
            //producer.send(new ProducerRecord<String, Transaction>("successful", 100, tx.getInBank(), tx)); //causes exception for testing
            producer.send(new ProducerRecord<String, Transaction>("successfulTX", tx.getInBankPartition(), tx.getInBank(), tx));
            producer.send(new ProducerRecord<String, Transaction>("successfulTX", tx.getOutBankPartition(), tx.getOutBank(), CompensationRecord(tx)));
            producer.send(new ProducerRecord<String, Transaction>("localBalance", tx.getInBankPartition(), tx.getInBank(), BalanceRecord(tx)));
        } else {
            producer.send(new ProducerRecord<String, Transaction>("rejectedTX", tx.getInBank(), tx));
            System.out.println("Big transaction cancelled.");
        }
    }

    private static void ProcessAggregated(Transaction tx) throws ExecutionException, InterruptedException {
        PollFromTmp(tx);
        if (bankBalance.get(tx.getInBank()) >= tx.getAmount()) {
            bankBalance.compute(tx.getInBank(), (key, value) -> value - tx.getAmount());
            producer.send(new ProducerRecord<String, Transaction>("localBalance", tx.getInBankPartition(), tx.getInBank(), BalanceRecord(tx)));
            producer.send(new ProducerRecord<String, Transaction>("bigTX", tx.getOutBankPartition(), tx.getOutBank(), CompensationRecord(tx)));
            producer.send(new ProducerRecord<String, Transaction>("successfulTX", tx.getInBankPartition(), tx.getInBank(), tx));
            producer.send(new ProducerRecord<String, Transaction>("successfulTX", tx.getOutBankPartition(), tx.getOutBank(), CompensationRecord(tx)));
        } else {
            producer.send(new ProducerRecord<String, Transaction>("bigTX", tx.getInBankPartition(), tx.getInBank(), tx));
            System.out.println("Aggregated transaction suspended. Sent back to big topic.");
        }
    }

    private static void ProcessCompensate(Transaction tx) throws ExecutionException, InterruptedException {
        PollFromTmp(tx);
        bankBalance.compute(tx.getInBank(), (key, value) -> value + tx.getAmount());
        producer.send(new ProducerRecord<String, Transaction>("localBalance", tx.getInBankPartition(), tx.getInBank(), BalanceRecord(tx)));
    }

    private static void InitBank(Transaction tx) throws ExecutionException, InterruptedException {
        bankBalance.put(tx.getInBank(), tx.getAmount());
        producer.send(new ProducerRecord<String, Transaction>("localBalance", tx.getInBankPartition(), tx.getInBank(), BalanceRecord(tx)));
        producer.send(new ProducerRecord<String, Transaction>("successfulTX", tx.getInBank(), tx));
        System.out.println("Bank " + tx.getInBank() + " has been initialized to balance " + tx.getAmount() + ".");
    }
}
