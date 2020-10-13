package com.waiyan.ume.kafka.producer;

import com.waiyan.ume.kafka.model.Transaction;
import com.waiyan.ume.kafka.serializer.TransactionSerializer;
import com.waiyan.ume.kafka.topic.Topic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProducerApp {

    private static final int MESSAGE_COUNT = 10;
    private static Logger logger = LoggerFactory.getLogger(ProducerApp.class);
    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool (1);
    private static List<Integer> customers = new ArrayList<>();
    private static List<String> products = new ArrayList<>();

    private static final Random rand = new Random();
    private static final Producer<Long, Transaction> txnProducer = ProducerCreator.createProducer(
            LongSerializer.class, TransactionSerializer.class);

    static {
        for (int i = 0; i < 10; i++) {
            customers.add(i);
            products.add(String.valueOf((char)(i + 0x41)));
        }
    }

    public static void main(String[] args) {
        Integer providedDelay = args.length > 0 ? Integer.valueOf(args[0]) : null;
        runProducer(providedDelay);
    }

    private static void runProducer(Integer providedDelay) {
        Runnable runnable = () -> {
            try {
                int randomNumber = rand.nextInt(MESSAGE_COUNT);
                for (int index = 0; index < MESSAGE_COUNT; index++) {
                    LocalDateTime now = LocalDateTime.now();
                    // Create delays (from 1 to 9 minutes) based on current minute
                    int delay = providedDelay == null ? now.getMinute() % 10 : providedDelay;
                    Transaction txn = generateTransaction(index == randomNumber ? now.minusMinutes(delay) : now);
                    ProducerRecord<Long, Transaction> record = new ProducerRecord<>(Topic.TXN.name(), (long) index, txn);

                    try {
                        RecordMetadata metadata = txnProducer.send(record).get();
                        logger.info("Record {} sent to partition {} with offset {}",
                                txn, metadata.partition(), metadata.offset());
                    } catch (ExecutionException | InterruptedException e) {
                        logger.error("Error in sending record: {}", e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                executor.shutdown();
                logger.error("Error in txnProducer: {}", e.getMessage(), e);
            }
        };
        executor.scheduleAtFixedRate(runnable,0L,30L, TimeUnit.SECONDS);
    }

    private static Transaction generateTransaction(LocalDateTime dt) {
        return new Transaction(dt, customers.get(rand.nextInt(customers.size())), products.get(rand.nextInt(products.size())));
    }
}
