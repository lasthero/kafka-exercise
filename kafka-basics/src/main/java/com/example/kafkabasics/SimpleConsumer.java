package com.example.kafkabasics;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {

  public static void main(String[] args) throws InterruptedException {
    Logger logger = LoggerFactory.getLogger(SimpleConsumer.class.getName());
    String bootstarpServers = "127.0.0.1:9092";
    String groupId = "my-conusumer-group";
    String topic = "first_topic";

    CountDownLatch latch = new CountDownLatch(1);

    Runnable consumerRunnable = new ConsumerThread(
        latch,
        bootstarpServers,
        groupId,
        topic
    );
    //start the thread
    Thread myThread = new Thread(consumerRunnable);
    myThread.start();

    //shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Caught shutdown hook");
      ((ConsumerThread)consumerRunnable).shutDown();
      try {
        latch.await();
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
      logger.info("Application has exited");
    }));

    try {
      latch.await();
    }
    catch (InterruptedException e) {
      logger.error("Application is interrupted", e);
    }
    finally {
      logger.info("Application is closing...");
    }
  }

  private static class ConsumerThread implements Runnable {

    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    public ConsumerThread(CountDownLatch latch,
        String bootstrapServers,
        String groupId,
        String topic)
    {
      this.latch = latch;
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      consumer = new KafkaConsumer<String, String>(properties);

      //subscribe
      consumer.subscribe(Arrays.asList(topic));
    }
    @Override
    public void run() {
      //poll data
      try {
        while (true) {
          ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
          consumerRecords.forEach(record -> logger.info(
              "Key: " + record.key() + "; Value:" + record.value() + "; Partition: " + record
                  .partition()));
          //logger.info();
        }
      }
      catch (WakeupException ex) {
        logger.info("Received shut down signal");
      }
      finally {
        consumer.close();
        latch.countDown();
      }
    }

    public void shutDown() {
      //interrupt consumer.poll(), throws WakeUpException
      consumer.wakeup();
    }
  }

}
