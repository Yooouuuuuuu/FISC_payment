# args[0]: # of partitions
# args[1]: # of transactions
# args[2]: "max.poll.records"
# args[3]: batch processing
# args[4]: poll from localBalance while repartition
# args[5]: credit topic exist
# args[6]: direct write to successful

i=750

while [ $i -le 2000 ]; do
    echo Number: $i

    echo "=== Initialize kafka topics === "
    # wait some time for deletion
/usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=46133:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar initialize 3 100000 $i true false false false

    echo "=== open validators & balancers === "
    for j in {1..3}; do
/usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=45715:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar validator 3 100000 $i true false false false &

/usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=44925:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar balancer 3 100000 $i true false false false &
    done

    sleep 30
    # wait some time for rebalance

    echo "=== input data === "
/usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=41255:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar sourceProducer 3 100000 $i true false false false &
    sleep 3600
    pkill -f 'automaton-1.11-8.jar'

    echo "=== consume, sort and write to csv === "
/usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=34703:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar consumeTimestamps 3 100000 $i true false false false

/usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=36905:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar sortTimestamps 3 100000 $i true false false false
    let "i+=250" 
done

