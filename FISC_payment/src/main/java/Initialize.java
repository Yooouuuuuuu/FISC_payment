import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

public class Initialize {
    public static void main(String[] args) throws Exception {

        //Remind: System.in.read() is in the code, press enter to continue.
        //inputs
        int numOfPartitions = Integer.parseInt(args[0]);
        short numOfReplicationFactor = 1;

        //props
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "off"); //"off", "trace", "debug", "info", "warn", "error".
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", "localhost:9092");
        AdminClient adminClient = KafkaAdminClient.create(adminProps);

        // delete topics
        adminClient.deleteTopics(Arrays.asList("smallTX", "bigTX", "localBalance", "successfulTX", "rejectedTX", "balance", "credit", "validateOffset", "aggregatedCredit"));
        //System.in.read();

        // create topics
        String topic_name1 = "bigTX";
        NewTopic topic_01 = new NewTopic(topic_name1, numOfPartitions, numOfReplicationFactor);

        String topic_name2 = "localBalance";
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy","compact");
        configs.put("delete.retention.ms","100");
        configs.put("segment.ms","100");
        configs.put("min.cleanable.dirty.ratio","0.01");
        NewTopic topic_02 = new NewTopic(topic_name2, numOfPartitions, numOfReplicationFactor);
        topic_02.configs(configs); //spacial configs for specific topic

        String topic_name3 = "successfulTX";
        NewTopic topic_03 = new NewTopic(topic_name3, numOfPartitions, numOfReplicationFactor);
        String topic_name4 = "rejectedTX";
        NewTopic topic_04 = new NewTopic(topic_name4, numOfPartitions, numOfReplicationFactor);
        String topic_name5 = "balance";
        NewTopic topic_05 = new NewTopic(topic_name5, numOfPartitions, numOfReplicationFactor);
        String topic_name6 = "smallTX";
        NewTopic topic_06 = new NewTopic(topic_name6, numOfPartitions, numOfReplicationFactor);
        String topic_name7 = "credit";
        NewTopic topic_07 = new NewTopic(topic_name7, numOfPartitions, numOfReplicationFactor);
        String topic_name8 = "validateOffset";
        NewTopic topic_08 = new NewTopic(topic_name8, numOfPartitions, numOfReplicationFactor);
        String topic_name9 = "aggregatedCredit";
        NewTopic topic_09 = new NewTopic(topic_name9, numOfPartitions, numOfReplicationFactor);
        Thread.sleep(10000); //wait 10 sec in case that the topic deletion is late
        CreateTopicsResult result = adminClient.createTopics(Arrays.asList(topic_01, topic_02, topic_03, topic_04, topic_05, topic_06, topic_07, topic_08, topic_09));

        // check if topic created successfully
        for(Map.Entry entry : result.values().entrySet()) {
            String topic_name = (String) entry.getKey();
            boolean success = true;
            String error_msg = "";
            try {
                ((KafkaFuture<Void>) entry.getValue()).get();
            } catch (Exception e) {
                success = false;
                error_msg = e.getMessage();
            }
            if (success)
                System.out.println("Topic: " + topic_name + " creation completed!");
            else
                System.out.println("Topic: " + topic_name + " creation fail, due to [" + error_msg + "]");
        }
        adminClient.close();

        // wait until validators and balancers are ready
        //System.in.read();

        // initialize
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "TxSerializer");
        Producer<String, Transaction> producer = new KafkaProducer<>(producerProps);

        ArrayList a = new ArrayList();

        int partitionNum = 0;
        Transaction transaction = new Transaction("822", "000", 100000000L, -1, 0, -1, 3);

        while (numOfPartitions > 0) { //numOfPartitions < 10
            transaction = new Transaction("10"+partitionNum, "000", 100000000L, -1, partitionNum, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction.getInBankPartition(), transaction.getInBank(), transaction)).get();

            partitionNum +=1;
            numOfPartitions -=1;
        }

        /*
        if (numOfPartitions >= 3) {
            Transaction transaction1 = new Transaction("822", "000", 100000000L, -1, 0, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction1.getInBankPartition(), transaction1.getInBank(), transaction1)).get();
            Transaction transaction2 = new Transaction("700", "000", 100000000L, -1, 1, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction2.getInBankPartition(), "700", transaction2)).get();
            Transaction transaction3 = new Transaction("013", "000", 100000000L, -1, 2, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction3.getInBankPartition(), "013", transaction3)).get();
        }

        if (numOfPartitions >= 10) {
            Transaction transaction4 = new Transaction("812", "000", 100000000L, -1, 3, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction4.getInBankPartition(), "812", transaction4)).get();
            Transaction transaction5 = new Transaction("006", "000", 100000000L, -1, 4, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction5.getInBankPartition(), "006", transaction5)).get();
            Transaction transaction6 = new Transaction("007", "000", 100000000L, -1, 5, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction6.getInBankPartition(), transaction6.getInBank(), transaction6)).get();
            Transaction transaction7 = new Transaction("004", "000", 100000000L, -1, 6, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction7.getInBankPartition(), transaction7.getInBank(), transaction7)).get();
            Transaction transaction8 = new Transaction("008", "000", 100000000L, -1, 7, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction8.getInBankPartition(), transaction8.getInBank(), transaction8)).get();
            Transaction transaction9 = new Transaction("012", "000", 100000000L, -1, 8, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction9.getInBankPartition(), transaction9.getInBank(), transaction9)).get();
            Transaction transaction10 = new Transaction("943", "000", 100000000L, -1, 9, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction10.getInBankPartition(), transaction10.getInBank(), transaction10)).get();
        }

        if (numOfPartitions >= 20) {
            Transaction transaction11 = new Transaction("000", "000", 100000000L, -1, 10, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction11.getInBankPartition(), transaction11.getInBank(), transaction11)).get();
            Transaction transaction12 = new Transaction("111", "000", 100000000L, -1, 11, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction12.getInBankPartition(), transaction12.getInBank(), transaction12)).get();
            Transaction transaction13 = new Transaction("222", "000", 100000000L, -1, 12, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction13.getInBankPartition(), transaction13.getInBank(), transaction13)).get();
            Transaction transaction14 = new Transaction("333", "000", 100000000L, -1, 13, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction14.getInBankPartition(), transaction14.getInBank(), transaction14)).get();
            Transaction transaction15 = new Transaction("444", "000", 100000000L, -1, 14, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction15.getInBankPartition(), transaction15.getInBank(), transaction15)).get();
            Transaction transaction16 = new Transaction("555", "000", 100000000L, -1, 15, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction16.getInBankPartition(), transaction16.getInBank(), transaction16)).get();
            Transaction transaction17 = new Transaction("666", "000", 100000000L, -1, 16, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction17.getInBankPartition(), transaction17.getInBank(), transaction17)).get();
            Transaction transaction18 = new Transaction("777", "000", 100000000L, -1, 17, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction18.getInBankPartition(), transaction18.getInBank(), transaction18)).get();
            Transaction transaction19 = new Transaction("888", "000", 100000000L, -1, 18, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction19.getInBankPartition(), transaction19.getInBank(), transaction19)).get();
            Transaction transaction20 = new Transaction("999", "000", 100000000L, -1, 19, -1, 3);
            producer.send(new ProducerRecord<String, Transaction>("bigTX", transaction20.getInBankPartition(), transaction20.getInBank(), transaction20)).get();
        }

         */
        System.out.println("Bank balance has been initialized.");
    }
}


