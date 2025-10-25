package ma.ensias;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer {
    public static void main(String[] args) throws Exception {

        // Verify topic is provided as argument
        if(args.length == 0){
            System.out.println("Entrer le nom du topic");
            return;
        }

        String topicName = args[0].toString();

        // Access producer configurations
        Properties props = new Properties();

        // Specify Kafka server
        props.put("bootstrap.servers", "localhost:9092");

        // Define acknowledgement for producer requests
        props.put("acks", "all");

        // If request fails, producer can retry automatically
        props.put("retries", 0);

        // Specify buffer size in config
        props.put("batch.size", 16384);

        // Control total memory space available to producer for buffering
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for(int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)));

        System.out.println("Message envoye avec succes");
        producer.close();
    }
}