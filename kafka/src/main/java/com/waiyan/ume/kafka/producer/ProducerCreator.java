package com.waiyan.ume.kafka.producer;

import java.util.Properties;

import com.waiyan.ume.kafka.IKafkaConstants;
import com.waiyan.ume.kafka.model.Transaction;
import com.waiyan.ume.kafka.serializer.TransactionSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;

public class ProducerCreator {
    public static Producer<Long, Transaction> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransactionSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}