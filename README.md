This repo is my exercise on Kafka by following Stephane Maarek's Udemy course. The solution takes tweets from twitter API then posts to Kafka using Kafka producers. Then a Kafka consumer consumes the twitter and posts to Bonsai ElasticSearch cluster

- Prerequisites:
  - Docker: I used docker to start Kafka and Zookeeper
  - Access to twitter API - twitter developer account
  - An account at Bonsai `https://app.bonsai.io/`
  
- To start Kafka:
`docker-compose up -d`
  - Use option `-d` to run container in background.
  - Use `docker-compose down` to stop

- Run Twitter producer:
  - Modify the tokens and secrets as well as twitter search terms at TwitterKafkaProducer class then start the application.
  - New tweets should be showing at console.
- Run ElasticSearch consumer:
  - Modify username and password to bonsai at ElasticSearchConsumer class
  - Start the application and tweets being consumed should be displaying on console.
  - Go to bonsai to verify tweets are posted to the cluster
- Filter Stream:
  - Filter Stream uses Kafka Stream to consume and filter tweets 

- Useful Kafka CLI commands on Docker:
  - Create topics:
``docker exec -it kafka_exercise_kafka_1 /usr/bin/kafka-topics --bootstrap-server 127.0.0.1:9092 --topic important_tweets --create --partitions 3 --replication-factor 1``

  - Start a consumer:
`docker exec -it kafka_exercise_kafka_1 /usr/bin/kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --group first_group --key-deserializer org.apache.kafka.common.serialization.StringDeserializer --value-deserializer org.apache.kafka.common.serialization.StringDeserializer --property print.key=true --property key.separator="-"`

  - Describe consumer groups:
``docker exec -it kafka_exercise_kafka_1 /usr/bin/kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group kafka-demo-elasticsearch --describe``

 - Reset consumer group offsets:
``docker exec -it kafka_exercise_kafka_1 /usr/bin/kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group kafka-demo-elasticsearch --reset-offsets --execute --to-earliest --topic twitter-tweets``

  - Start a producer:
  `docker exec -it kafka_exercise_kafka_1 /usr/bin/kafka-console-producer  --bootstrap-server 127.0.0.1:9092 --topic first_topic`