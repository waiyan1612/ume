package com.waiyan.ume.kafka.consumer;

import com.waiyan.ume.kafka.IKafkaConstants;
import com.waiyan.ume.kafka.model.Transaction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ConsumerApp {
    public static void main(String[] args) {
        runConsumer();
    }
    private static void runConsumer() {
        Consumer<Long, Transaction> consumer = ConsumerCreator.createConsumer();
        int noMessageFound = 0;
        while (true) {
            ConsumerRecords<Long, Transaction> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            consumerRecords.forEach(record -> {
                System.out.println("Record " + record.value() + " received from partition " + record.partition() + " with offset " + record.offset());
            });
            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
    }
}