package com.waiyan.ume.kafka.producer;

import com.waiyan.ume.kafka.ConnectionChecker;
import com.waiyan.ume.kafka.model.Transaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;


class ProducerCreator {

    private static Logger logger = LoggerFactory.getLogger(ProducerCreator.class);

    static Producer<Long, Transaction> createProducer() {
         return createProducer(StringDeserializer.class, StringDeserializer.class);
    }

    static Producer<Long, Transaction> createProducer(Class keySer, Class valueSer) {
        Properties properties = new Properties();
        try {
            String path = (Objects.requireNonNull(ConnectionChecker.class.getClassLoader()
                    .getResource("kafka-producer.properties")).getPath());
            properties.load(new FileReader(new File(path)));
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer.getName());
        } catch (IOException e) {
            logger.error("Error occurred while loading properties", e);
        }
        return new KafkaProducer<>(properties);
    }
}
