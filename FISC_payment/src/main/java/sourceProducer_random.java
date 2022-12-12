import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

public class sourceProducer_random {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "off"); //"off", "trace", "debug", "info", "warn", "error".

        //inputs
        int numOfPartitions = Integer.parseInt(args[0]); //3
        long numOfData = Integer.parseInt(args[1]); //10000
        boolean bigTXOnly = true;
        HashMap<String, Long> bankBalance = new HashMap<String, Long>();
        ArrayList<String> bank = new ArrayList<String>();

        int count = 0;
        while(count < numOfPartitions){
            bank.add("10" + count);
            bankBalance.put("10" + count, 100000000L);
            count += 1;
        }

        //bankBalance.put("700", 100000000L);
        //bankBalance.put("013", 100000000L);


        //create producer properties
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "TxSerializer");

        //create the producer
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(properties);

        //choose partition option
        /*
        String[] bank = {"822", "700", "013"};
        if (numOfPartitions == 10) {
            bank = new String[]{"822", "700", "013", "812", "006", "007", "004", "008", "012", "943"};
        } else if (numOfPartitions == 20) {
            bank = new String[]{"822", "700", "013", "812", "006", "007", "004", "008", "012", "943", "000", "111", "222", "333", "444", "555", "666", "777", "888", "999"};
        }
        */

        long amount;
        for (long i = 0; i < numOfData; i++) { //3.5% of the transactions are big transactions

            //whether we use aggregator
            if (bigTXOnly) {
                amount = 50L;
            } else {
                if (Math.random() <= 0.035) {
                    amount = 50L;
                } else {
                    amount = 1L;
                }
            }

            //random inBank, outBank
            int inBankNum = ThreadLocalRandom.current().nextInt(0, numOfPartitions);
            int outBankNum = ThreadLocalRandom.current().nextInt(0, numOfPartitions);
            Transaction output = new Transaction(bank.get(inBankNum), bank.get(outBankNum), amount, i, inBankNum, outBankNum, 0);

            //String topic;
            if (amount == 50L) {
                producer.send(new ProducerRecord<String, Transaction>("bigTX", output.getInBankPartition(), output.getInBank(), output));
            } else if (amount == 1){
                producer.send(new ProducerRecord<String, Transaction>("smallTX", output.getInBankPartition(), output.getInBank(), output));
            }

            bankBalance.compute(output.getInBank(), (key, value) -> value - output.getAmount());
            bankBalance.compute(output.getOutBank(), (key, value) -> value + output.getAmount());
        }

        //flush and close producer
        producer.flush();
        producer.close();

        System.out.println(bankBalance);
    }
}



