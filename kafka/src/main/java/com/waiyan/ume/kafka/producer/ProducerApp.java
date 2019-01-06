package com.waiyan.ume.kafka.producer;
import java.time.LocalDate;
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
    private static final int YEAR = 2019;
    private static final int MONTH = 1;

    static {
        for(int i=0; i< 10; i++) {
            customers.add(i);
            products.add(String.valueOf((char)(i + 0x41)));
        }
    }

    public static void main(String[] args) {
        runProducer();
    }

    private static void runProducer() {
        Producer<Long, Transaction> producer = ProducerCreator.createProducer();
        Runnable r = new Runnable () {
            @Override
            public void run() {
                try {
                    int currentMinute = LocalDateTime.now().getMinute();
                    Random random = new Random();
                    int randomNumber = random.nextInt(IKafkaConstants.MESSAGE_COUNT);
                    for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
                        Transaction txn = index == randomNumber ?
                                generateTransaction(0) :
                                generateTransaction(currentMinute % 30 + 1);
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
                    System.out.println(e.getMessage());
                    System.out.println(e);
                }
            }
        };
        executor.scheduleAtFixedRate (r,0L,1L, TimeUnit.MINUTES);
    }

    private static Transaction generateTransaction(int day) {
        return new Transaction(
                day == 0 ? getRandPurchasedDate() : LocalDate.of(YEAR, MONTH, day),
                getRandCustomer(),
                getRandProduct()
        );
    }

    private static LocalDate getRandPurchasedDate() {
        long start = LocalDate.of(YEAR, MONTH, 1).toEpochDay();
        long end = LocalDate.of(YEAR, MONTH, 31).toEpochDay();
        long random = ThreadLocalRandom.current().nextLong(start, end);
        return LocalDate.ofEpochDay(random);
    }

    private static int getRandCustomer() {
        Random rand = new Random();
        int randomIndex = rand.nextInt(customers.size());
        return customers.get(randomIndex);
    }

    private static String getRandProduct() {
        Random rand = new Random();
        int randomIndex = rand.nextInt(products.size());
        return products.get(randomIndex);
    }
}