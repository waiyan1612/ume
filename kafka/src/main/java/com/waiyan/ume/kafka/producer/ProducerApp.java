package com.waiyan.ume.kafka.producer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import com.waiyan.ume.kafka.IKafkaConstants;
import com.waiyan.ume.kafka.model.Transaction;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerApp {

    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool (1);
    private static List<Integer> customers = new ArrayList<>();
    private static List<String> products = new ArrayList<>();

    static {
        for(int i=0; i< 10; i++) {
            customers.add(i);
            products.add(String.valueOf((char)(i + 0x41)));
        }
    }

    public static void main(String[] args) {
        Integer providedDelay = args.length > 0 ? Integer.valueOf(args[0]) : null;
        runProducer(providedDelay);
    }

    private static void runProducer(Integer providedDelay) {
        Producer<Long, Transaction> producer = ProducerCreator.createProducer();
        Runnable r = new Runnable () {
            @Override
            public void run() {
                try {
                    Random random = new Random();
                    int randomNumber = random.nextInt(IKafkaConstants.MESSAGE_COUNT);
                    for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
                        LocalDateTime now = LocalDateTime.now();
                        // Create delays (from 1 to 9 minutes) based on current minute
                        int delay = providedDelay == null ? now.getMinute() % 10 : providedDelay;
                        Transaction txn = generateTransaction(index == randomNumber ? now.minusMinutes(delay) : now);
                        ProducerRecord<Long, Transaction> record = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, (long)index, txn);

                        try {
                            RecordMetadata metadata = producer.send(record).get();
                            System.out.println("Record " + txn + " sent to partition " + metadata.partition() + " with offset " + metadata.offset());
                        }
                        catch (ExecutionException | InterruptedException e) {
                            System.out.println("Error in sending record");
                            System.out.println(e);
                        }
                    }
                } catch (Exception e) {
                    executor.shutdown();
                    e.printStackTrace();
                }
            }
        };
        executor.scheduleAtFixedRate (r,0L,30L, TimeUnit.SECONDS);
    }

    private static Transaction generateTransaction(LocalDateTime dt) {
        Random rand = new Random();
        return new Transaction(dt, customers.get(rand.nextInt(customers.size())), products.get(rand.nextInt(products.size())));
    }
}