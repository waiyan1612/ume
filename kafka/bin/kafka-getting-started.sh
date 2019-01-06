// Kafka Installation Dir
cd /opt/cloudera/parcels/KAFKA-3.1.0-1.3.1.0.p0.35/lib/kafka/bin/

// Topics on Zookeeper
./kafka-topics.sh --list --zookeeper 10.111.1.13:2181
./kafka-topics.sh --create --zookeeper 10.111.1.13:2181 --replication-factor 1 --partitions 1 --topic ume
./kafka-topics.sh --delete --zookeeper 10.111.1.13:2181 --topic ume


// Sending messages via Brokers
./kafka-console-producer.sh --broker-list 10.111.1.13:9092 --topic ume

// Receiving messages
./kafka-console-consumer.sh --bootstrap-server 10.111.1.13:9092 --topic ume --from-beginning

// Sample Producer
java -cp kafka-assembly-0.0.1-SNAPSHOT.jar com.waiyan.ume.kafka.producer.ProducerApp

// Sample Consumer
java -cp kafka-assembly-0.0.1-SNAPSHOT.jar com.waiyan.ume.kafka.consumer.ConsumerApp
