import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class SourceProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException{
        Logger logger = LoggerFactory.getLogger(SourceProducer.class);
        String bootstrapServers = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "TxSerializer");

        String[] bank = {"822","700","013"};
        //create the producer
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(properties);

        //822 to 700
        //random generate 3.5% big
        //50, 1


        File doc = new File("/home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/src/main/java/output.txt");
        Scanner obj = null;
        try {
            obj = new Scanner(doc);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while (obj.hasNextLine()) {
            String[] transaction = extractStringData(obj.nextLine());

            //System.out.println(Integer.parseInt(transaction[8]));


            int amount = Integer.parseInt(transaction[8]);

            int inbankPartition = 0;
            int outbankPartition = 0;
            for(int i=0; i < bank.length; i++){
                if (transaction[6].equals(bank[i])){
                    inbankPartition = i;
                }
            }
            for(int j=0; j < bank.length; j++){
                if (transaction[7].equals(bank[j])){
                    outbankPartition = j;
                }
            }
            String topic;
            Transaction transaction1 = new Transaction(transaction[6],transaction[7],Long.parseLong(transaction[8]),Long.parseLong(transaction[11]),inbankPartition,outbankPartition,0);
            if (amount > 5000000){
                topic = "bigTX"; //big
            }else{
                topic = "smallTX"; //small
            }

            ProducerRecord<String, Transaction> record = new ProducerRecord<>(topic, inbankPartition, transaction1.getInBank(), transaction1);
            sendData(record, producer, logger);
        }
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }


    private static String[] extractStringData(String dataString){
        String[] splitlist = dataString.split(",");
        //System.out.println(splitlist[3]);
        return splitlist;
    }

    private static void sendData(ProducerRecord record, KafkaProducer producer, Logger logger){
        //send data-asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Key:" + record.key()+ "\n" +
                            "Value:" + record.value()+ "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }
        });
    }
}

