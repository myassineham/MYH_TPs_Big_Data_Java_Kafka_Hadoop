package ma.ensias;


import org.apache.kafka.clients.producer.*;
import java.util.*;

public class WordProducer {
    public static void main(String[] args) throws Exception {
        if(args.length == 0){
            System.out.println("Usage: WordProducer <topic-name>");
            return;
        }

        String topicName = args[0];
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);

        System.out.println("Tapez vos mots (Ctrl+C pour quitter):");

        while(scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] words = line.split("\\s+");

            for(String word : words) {
                if(!word.isEmpty()) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(topicName, word, word);
                    producer.send(record);
                    System.out.println("Envoyé: " + word);
                }
            }
        }

        producer.close();
        scanner.close();
    }
}