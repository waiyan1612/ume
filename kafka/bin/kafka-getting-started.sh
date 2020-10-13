#!/bin/sh

# Get kafka docker-compose file from https://github.com/waiyan1612/rtpa/blob/master/docker-compose-kafka.yml
# Log into docker container
docker container exec -it rtpa_kafka_1 /bin/bash

# Kafka Installation Dir
cd /opt/bitnami/kafka/bin/

# Topics on Zookeeper
./kafka-topics.sh --list --zookeeper zookeeper:2181
./kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic ume
./kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic ume


# Sending messages via Brokers
./kafka-console-producer.sh --broker-list zookeeper:9092 --topic ume

# Receiving messages
./kafka-console-consumer.sh --bootstrap-server zookeeper:9092 --topic ume --from-beginning

# Sample Producer with 0 min delay
java -cp kafka-assembly-0.0.1-SNAPSHOT.jar com.waiyan.ume.kafka.producer.ProducerApp 0
# Sample Producer with varying delay
java -cp kafka-assembly-0.0.1-SNAPSHOT.jar com.waiyan.ume.kafka.producer.ProducerApp

# Sample Consumer
java -cp kafka-assembly-0.0.1-SNAPSHOT.jar com.waiyan.ume.kafka.consumer.ConsumerApp
