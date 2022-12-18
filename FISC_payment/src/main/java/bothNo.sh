# 宣告使用 /bin/bash
#!/bin/bash

i=1

while [ $i -le 10 ]; do
    echo Number: $i

    echo "=== Initialize kafka topics === "
    # wait some time for deletion
/usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=42175:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar Initialize 3

    echo "=== open validators & balancers === "
    for j in {1..3}; do
/usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=45821:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar validator_withoutBatch_PollLocal 500 &

/usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=33809:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar balancer 500 &
    done

    sleep 30
    # wait some time for rebalance

    echo "=== input data === "
/usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=34463:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar sourceProducer_random 3 100000 &
    sleep 3600
    pkill -f 'automaton-1.11-8.jar'

    echo "=== consume, sort and write to csv === "
    /usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=37305:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar consume_timestamp 100000

/usr/lib/jvm/java-11-amazon-corretto/bin/java -javaagent:/home/yooouuuuuuu/idea-IC-221.5921.22/lib/idea_rt.jar=39227:/home/yooouuuuuuu/idea-IC-221.5921.22/bin -Dfile.encoding=UTF-8 -classpath /home/yooouuuuuuu/IdeaProjects/kafka/FISC_payment/target/classes:/home/yooouuuuuuu/.m2/repository/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar:/home/yooouuuuuuu/.m2/repository/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar:/home/yooouuuuuuu/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:/home/yooouuuuuuu/.m2/repository/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar:/home/yooouuuuuuu/.m2/repository/com/github/javafaker/javafaker/1.0.2/javafaker-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar:/home/yooouuuuuuu/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23-android.jar:/home/yooouuuuuuu/.m2/repository/com/github/mifmif/generex/1.0.2/generex-1.0.2.jar:/home/yooouuuuuuu/.m2/repository/dk/brics/automaton/automaton/1.11-8/automaton-1.11-8.jar sortTimestamps_credit 3 100000 500
    let "i+=1" 
done

