package ma.ensias;


import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

public class WordCountConsumer {
    public static void main(String[] args) throws Exception {
        if(args.length == 0){
            System.out.println("Usage: WordCountConsumer <topic-name>");
            return;
        }

        String topicName = args[0];
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "word-count-group");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

        Map<String, Integer> wordCounts = new HashMap<>();

        System.out.println("En attente de messages...");

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records) {
                String word = record.value();
                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);

                System.out.println("\n=== Comptage des mots ===");
                wordCounts.forEach((w, count) ->
                        System.out.println(w + ": " + count));
            }
        }
    }
}