package com.waiyan.ume.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.waiyan.ume.kafka.model.Transaction;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class TransactionDeserializer implements Deserializer<Transaction> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey){

    }

    @Override
    public Transaction deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        Transaction object = null;
        try {
            object = mapper.readValue(data, Transaction.class);
        } catch (Exception exception) {
            System.out.println("Error in deserializing bytes "+ exception);
        }
        return object;
    }

    @Override
    public void close() {

    }
}
