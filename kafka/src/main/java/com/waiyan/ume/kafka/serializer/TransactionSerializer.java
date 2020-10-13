package com.waiyan.ume.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.waiyan.ume.kafka.model.Transaction;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TransactionSerializer implements Serializer<Transaction> {

    private static Logger logger = LoggerFactory.getLogger(TransactionSerializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey){

    }

    @Override
    public byte[] serialize(String topic, Transaction data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        try {
            retVal = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception e) {
            logger.error("Error in serializing object {}", data, e);
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
