`docker exec -it kafka_exercise_kafka_1 /usr/bin/kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --group first_group --key-deserializer org.apache.kafka.common.serialization.StringDeserializer --value-deserializer org.apache.kafka.common.serialization.StringDeserializer --property print.key=true --property key.separator="-"`


* Describe groups:
``docker exec -it kafka_exercise_kafka_1 /usr/bin/kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group kafka-demo-elasticsearch --describe``


* Reset offsets:
``docker exec -it kafka_exercise_kafka_1 /usr/bin/kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group kafka-demo-elasticsearch --reset-offsets --execute --to-earliest --topic twitter-tweets``

* Create topics:
``docker exec -it kafka_exercise_kafka_1 /usr/bin/kafka-topics --bootstrap-server 127.0.0.1:9092 --topic important_tweets --create --partitions 3 --replication-factor 1``