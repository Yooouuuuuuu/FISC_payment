package old;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class sortTimestamps1 {
    public static void main(String[] args) throws IOException {

        //inputs
        int numOfPartitions = Integer.parseInt(args[0]);
        int numOfTX = Integer.parseInt(args[1]);
        int maxPollRecords = Integer.parseInt(args[2]);

        //create a map for serialNumber and timeStamps pair
        Map<String, ArrayList<String>> mapOfTimeStamps = new HashMap<>();
        ArrayList<String> lists[] = new ArrayList[numOfTX + 1];
        for (int i = 0; i <= numOfTX; i++) {
            lists[i] = new ArrayList<>();
        }

        //read from bixTX.txt and put into mapOfTimeStamps
        List<String> bigTX;
        bigTX = Files.readAllLines(Paths.get("/home/yooouuuuuuu/kafka_projects/TXtimestamps/bigTX.txt"), StandardCharsets.UTF_8);
        long numOfData = 2L * numOfTX + 2L * numOfPartitions; //without credit data, half of the records.
        for (int i = 0; i < (2L * numOfTX + 2L * numOfPartitions); i += 2) {
            lists[Integer.parseInt(bigTX.get(i))].add(bigTX.get(i + 1));
            mapOfTimeStamps.put(bigTX.get(i), lists[Integer.parseInt(bigTX.get(i))]);
        }
        //read from balance.txt and put into mapOfTimeStamps
        List<String> balance;
        balance = Files.readAllLines(Paths.get("/home/yooouuuuuuu/kafka_projects/TXtimestamps/balance.txt"), StandardCharsets.UTF_8);
        for (int i = 0; i < (4L * numOfTX + 2L * numOfPartitions); i += 2) {
            lists[Integer.parseInt(balance.get(i))].add(balance.get(i + 1));
            mapOfTimeStamps.put(balance.get(i), lists[Integer.parseInt(balance.get(i))]);
        }

        //timestamps of initializing
        //System.out.println(mapOfTimeStamps.get("10000"));
        //timestamps of specific TX
        //System.out.println(mapOfTimeStamps.get("0"));
        //System.out.println(mapOfTimeStamps.get("9999"));

        //average of time between bigToSuccessful

        /*
        long bigToSuccessful=0;
        for (int i = 0; i < numOfTX; i++) {
            //System.out.println(i);
            bigToSuccessful += Long.parseLong(mapOfTimeStamps.get(String.valueOf(i)).get(0)) - Long.parseLong(mapOfTimeStamps.get(String.valueOf(i)).get(1));
        }
        bigToSuccessful = Math.abs(bigToSuccessful/numOfTX);
        System.out.println("average of time between bigToSuccessful is: " + bigToSuccessful + " ms");

        //average of time between successfulToBalance
        long successfulToBalance=0;
        for (int i = 0; i < numOfTX; i++) {
            successfulToBalance += Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(1)) -
                    Math.max((Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(2))), (Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(3))));
        }
        successfulToBalance = Math.abs(successfulToBalance/numOfTX);
        System.out.println("average of time between successfulToBalance is: " + successfulToBalance + " ms");
*/


        //average of time between bigToBalance
        long bigToBalance = 0;
        for (int i = 0; i < numOfTX; i++) {
            bigToBalance += Math.max((Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(1))), (Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(2)))) -
                    Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(0));
        }
        bigToBalance = bigToBalance/numOfTX;
        System.out.println("average of time between bigToBalance is: " + bigToBalance + " ms");

        long tps;
        tps =   Math.max(Long.parseLong(mapOfTimeStamps.get(String.valueOf(numOfTX-1)).get(1)), Long.parseLong(mapOfTimeStamps.get(String.valueOf(numOfTX-1)).get(2))) -
                Long.parseLong(mapOfTimeStamps.get(String.valueOf(0)).get(0));
        tps = numOfTX/(tps/1000);
        System.out.println("tps is: " + tps);

        //delay on specific TX
        /*
        for (int i = 0; i < numOfTX; i += numOfTX/10) {
            bigToSuccessful = Math.abs(Long.parseLong(mapOfTimeStamps.get(String.valueOf(i)).get(0)) - Long.parseLong(mapOfTimeStamps.get(String.valueOf(i)).get(1)));
            successfulToBalance = Math.abs(Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(1)) -
                    Math.max((Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(2))), (Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(3)))));
            long bigToBalance = bigToSuccessful + successfulToBalance;
            System.out.println(i + "th TX: " + "bigToSuccessful: " + bigToSuccessful + " successfulToBalance: " + successfulToBalance + " bigToBalance: " + bigToBalance);
        }
        
         */

        //write to csv
        BufferedWriter bw = new BufferedWriter(new FileWriter("/home/yooouuuuuuu/kafka_projects/TXtimestamps/"+ maxPollRecords +"records.csv"));//檔案輸出路徑
        bw.write("TransactionID" + "," + "bigTX" + ","  + "balance");//寫到新檔案中

        for (int i = 0; i < numOfTX; i += 1) {
            bw.newLine();//新起一行
            String[] data = {
                    String.valueOf(Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(0))),
                    String.valueOf(Math.max((Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(1))), (Long.parseLong(mapOfTimeStamps.get(Integer.toString(i)).get(2))))),};
            //System.out.println(data[0] + data[1] + data[2]);
            bw.write(i+1 + "," + data[0] + "," + data[1]);//寫到新檔案中
            }
        bw.close();

        //append to csv
        File file = new File("/home/yooouuuuuuu/kafka_projects/TXtimestamps/timeStamps.csv");
        FileWriter fr = new FileWriter(file, true);
        BufferedWriter br = new BufferedWriter(fr);
        br.newLine();

        br.write(maxPollRecords + "," + bigToBalance + "," + tps );
        br.close();
        fr.close();
    }
}