package com.waiyan.ume.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class ConnectionChecker {

    private static Logger logger = LoggerFactory.getLogger(ConnectionChecker.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        try {
            String path = (Objects.requireNonNull(ConnectionChecker.class.getClassLoader()
                    .getResource("kafka-consumer.properties")).getPath());
            properties.load(new FileReader(new File(path)));
        } catch (IOException e) {
            logger.error("Error occurred while loading properties", e);
        }
        try (KafkaConsumer<String, String> simpleConsumer = new KafkaConsumer<>(properties)) {
            logger.info("Listing topics ... ");
            simpleConsumer.listTopics().keySet().forEach(x -> logger.info(x));
        }
    }
}
