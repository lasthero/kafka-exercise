package com.example.kafka.stream.filter.tweets;

import com.google.gson.JsonParser;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;

public class StreamFilterTweets {
  private static JsonParser jsonParser = new JsonParser();

  public static void main(String[] args) {

    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-filter-tweets");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

    //create a topology
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> inputTopic = builder.stream("twitter_tweets");
    KStream<String, String> filteredStream = inputTopic.filter(
        (k, jsonTweet) -> extractUserFollowersFromTweet(jsonTweet) > 1000
    );

    filteredStream.to("important_tweets");

    //build topology
    KafkaStreams streams = new KafkaStreams(builder.build(), properties);

    streams.start();
  }


  private static Integer extractUserFollowersFromTweet(String tweetJson) {
    try {
      return jsonParser.parse(tweetJson)
          .getAsJsonObject()
          .get("user").getAsJsonObject().get("followers_count")
          .getAsInt();
    }
    catch (NullPointerException e) {
      return 0;
    }
  }

}
