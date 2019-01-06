package com.waiyan.ume.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.waiyan.ume.kafka.model.Transaction;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class TransactionSerializer implements Serializer<Transaction> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey){

    }

    @Override
    public byte[] serialize(String topic, Transaction data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        try {
            retVal = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception exception) {
            System.out.println("Error in serializing object"+ data);
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
