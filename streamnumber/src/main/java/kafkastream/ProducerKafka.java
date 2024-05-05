package kafkastream;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class ProducerKafka {

public static void main(String[] args) throws Exception {

        // Verify that the topic is given as an argument
        if (args.length == 0) {
            System.out.println("Enter the Kafka topic name as an argument.");
            return;
        }

        // Assign the topic name to a variable
        String topicName = args[0];

        // Create properties instance to access Kafka producer configurations
        Properties props = new Properties();

        // Assign Kafka server identifier
        props.put("bootstrap.servers", "localhost:9092");

        // Define an acknowledgment for producer requests
        props.put("acks", "all");

        // If the request fails, the producer can retry automatically
        props.put("retries", 0);

        // Specify the batch size in the configuration
        props.put("batch.size", 16384);

        // Buffer.memory controls the total amount of memory available to the producer for buffering
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create a Kafka producer instance
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Path to the CSV file
        String csvFilePath = "streamnumber/src/main/resources/input/Spotify_cleaned.csv";
        //String csvFilePath = "Spotify_cleaned.csv";


        try (FileReader fileReader = new FileReader(csvFilePath);
             CSVParser csvParser = CSVFormat.DEFAULT.parse(fileReader)) {

            // Iterate over CSV records
            for (CSVRecord csvRecord : csvParser) {
                StringBuilder message = new StringBuilder();

                // Concatenate values from each column
                for (String value : csvRecord) {
                    if (message.length() > 0) {
                        message.append(", "); // Add a comma and a space between the values
                    }
                    message.append(value);
                }

                // Send the message to the Kafka topic
                producer.send(new ProducerRecord<>(topicName, message.toString()));
                System.out.println("Message sent successfully: " + message.toString());

                Thread.sleep(1000);
            }

            // Close the Kafka producer
            producer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

