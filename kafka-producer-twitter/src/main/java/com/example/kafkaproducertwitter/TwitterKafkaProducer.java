package com.example.kafkaproducertwitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterKafkaProducer {

  public static void main(String[] args) throws InterruptedException {

    Runnable twitterRunnable = new TwitterClientRunnable();
    //start the thread
    Thread myThread = new Thread(twitterRunnable);
    myThread.start();
  }

  private static class TwitterClientRunnable implements Runnable {

    private Logger logger = LoggerFactory.getLogger(TwitterKafkaProducer.class.getName());
    private BlockingQueue<String> msgQueue;
    //private BlockingQueue<Event> eventQueue;
    private KafkaProducer<String, String> producer;
    private Client hosebirdClient;
    //modify these setting before run
    private final String consumerKey = "key";
    private final String consumerSecret = "secret";
    private final String token="token";
    private final String tokenSecret="secret";

    public TwitterClientRunnable() throws InterruptedException {
      /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
      msgQueue = new LinkedBlockingQueue<String>(100000);
      //eventQueue = new LinkedBlockingQueue<Event>(1000);
      producer = createKafkaProducer();
      connect();
    }

    private void connect() throws InterruptedException {

      /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
      Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
      StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
      // Optional: set up some followings and track terms
      // List<Long> followings = Lists.newArrayList(139305555L, 997936140433829889L);
      List<String> terms = Lists.newArrayList("kafka", "#apachekafka");
      //hosebirdEndpoint.followings(followings);
      hosebirdEndpoint.trackTerms(terms);

      // These secrets should be read from a config file
      Authentication hosebirdAuth = new OAuth1(consumerKey,
          consumerSecret,
          token,
          tokenSecret);

      ClientBuilder builder = new ClientBuilder()
          .name("Hosebird-Client-01")                              // optional: mainly for the logs
          .hosts(hosebirdHosts)
          .authentication(hosebirdAuth)
          .endpoint(hosebirdEndpoint)
          .processor(new StringDelimitedProcessor(msgQueue));

      hosebirdClient = builder.build();
      // Attempts to establish a connection.
      hosebirdClient.connect();
    }

    private KafkaProducer<String, String> createKafkaProducer() {

      String bootstrapServers = "127.0.0.1:9092";
      Properties properties = new Properties();
      properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      return new KafkaProducer<String, String>(properties);

    }
    @Override
    public void run() {

      //shutdown hook
      Runtime.getRuntime().addShutdownHook(new Thread(()->{
        logger.info("Stopping the application");
        logger.info("Shutting down twitter client");
        hosebirdClient.stop();
        logger.info("Closing producer...");
        producer.close();
        logger.info("DONE");
      }));
      // on a different thread, or multiple different threads....
      while (!hosebirdClient.isDone()) {
        String msg = null;
        try {
          msg = msgQueue.poll(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
          e.printStackTrace();
          hosebirdClient.stop();
        }
        if (msg != null) {
          logger.info(msg);
          producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              if (e != null) {
                logger.error("ERROR", e);
              }
            }
          });
        }

       //System.out.println("##### Message: " + msg);
      }

    }
  }
}
