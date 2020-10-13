package com.waiyan.ume.kafka.consumer;

import com.waiyan.ume.kafka.ConnectionChecker;
import com.waiyan.ume.kafka.model.Transaction;
import com.waiyan.ume.kafka.topic.Topic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ConsumerCreator {

   private static Logger logger = LoggerFactory.getLogger(ConsumerCreator.class);

   static Consumer<Long, Transaction> createConsumer() {
      List<String> allTopics = Stream.of(Topic.values()).map(Enum::name).collect(Collectors.toList());
      return createConsumer(StringDeserializer.class, StringDeserializer.class, allTopics);
   }

   static Consumer<Long, Transaction> createConsumer(Class keyDeser, Class valueDeser, List<String> topics) {

      Properties properties = new Properties();
      try {
         String path = (Objects.requireNonNull(ConnectionChecker.class.getClassLoader()
                 .getResource("kafka-consumer.properties")).getPath());
         properties.load(new FileReader(new File(path)));
         properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeser.getName());
         properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeser.getName());
      } catch (IOException e) {
         logger.error("Error occurred while loading properties", e);
      }

      Consumer<Long, Transaction> consumer = new KafkaConsumer<>(properties);


      consumer.subscribe(topics);
      return consumer;
   }
}
