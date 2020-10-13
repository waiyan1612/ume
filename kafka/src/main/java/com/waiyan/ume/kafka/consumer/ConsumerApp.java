package com.waiyan.ume.kafka.consumer;

import com.waiyan.ume.kafka.model.Transaction;
import com.waiyan.ume.kafka.serializer.TransactionDeserializer;
import com.waiyan.ume.kafka.topic.Topic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class ConsumerApp {

    private static final Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    private static Logger logger = LoggerFactory.getLogger(ConsumerApp.class);

    private static void runConsumer() {
        List<String> topics = List.of(Topic.TXN.name());
        Consumer<Long, Transaction> consumer = ConsumerCreator.createConsumer(
                LongDeserializer.class, TransactionDeserializer.class, topics);
        int noMessageFound = 0;
        while (true) {
            ConsumerRecords<Long, Transaction> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > MAX_NO_MESSAGE_FOUND_COUNT) {
                    // If no message found count is reached to threshold exit loop.
                    break;
                }
            }
            consumerRecords.forEach(record -> {
                logger.info("Record {} received from partition {} with offset {}", record.value(), record.partition(), record.offset());
            });
            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
    }

    public static void main(String[] args) {
        runConsumer();
    }
}
