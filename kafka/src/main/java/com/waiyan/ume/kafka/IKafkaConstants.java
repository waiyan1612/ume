package com.waiyan.ume.kafka;

public interface IKafkaConstants {
    public static String KAFKA_BROKERS = "10.111.1.13:9092";
    public static Integer MESSAGE_COUNT = 10;
    public static String CLIENT_ID = "waiyan";
    public static String TOPIC_NAME = "ume";
    public static String GROUP_ID_CONFIG = "consumer_waiyan";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    public static String OFFSET_RESET_LATEST = "latest";
    public static String OFFSET_RESET_EARLIER = "earliest";
    public static Integer MAX_POLL_RECORDS = 1;
}