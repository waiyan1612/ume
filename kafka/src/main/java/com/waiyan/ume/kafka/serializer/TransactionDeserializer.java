package com.waiyan.ume.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.waiyan.ume.kafka.model.Transaction;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TransactionDeserializer implements Deserializer<Transaction> {

    private static Logger logger = LoggerFactory.getLogger(TransactionDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey){

    }

    @Override
    public Transaction deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        Transaction object = null;
        try {
            object = mapper.readValue(data, Transaction.class);
        } catch (Exception e) {
            logger.error("Error in deserializing bytes", e);
        }
        return object;
    }

    @Override
    public void close() {

    }
}
