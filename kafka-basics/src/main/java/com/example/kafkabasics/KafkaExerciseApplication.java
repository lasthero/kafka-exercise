package com.example.kafkabasics;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaExerciseApplication {

  private static Logger logger = LoggerFactory.getLogger(KafkaExerciseApplication.class);

  public static void main(String[] args) throws ExecutionException, InterruptedException  {
    //SpringApplication.run(KafkaExerciseApplication.class, args);
    //createProducer();
  }

  private static void createProducer() throws ExecutionException, InterruptedException {
    String bootstarpServers = "127.0.0.1:9092";
    String key = "key_1";
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstarpServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //create a producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    //create producer record
    ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", key,"Hello world there!");
    //send data - async
    producer.send(record, (recordMetadata, e) -> {
      if (e == null) {
        logger.info("Received metadata. \n" +
            "Topic: " + recordMetadata.topic() + "\n" +
            "Partition: " + recordMetadata.partition() + "\n" +
            "Offset: " + recordMetadata.offset() + "\n" +
            "Timestamp: " + recordMetadata.timestamp());
      } else {
        logger.error("Error: " + e.getMessage());
      }
    }).get(); //make it synchronous; not in production

    producer.flush();
    producer.close();
  }

  private static void createConsumer() {

  }
}
