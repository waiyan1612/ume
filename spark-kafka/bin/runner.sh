export SPARK_KAFKA_VERSION=0.10

spark2-submit \
--master yarn \
--deploy-mode client \
--class "com.waiyan.ume.spark.streaming.KafkaConsumer" \
/home/waiyan/kafka/spark-kafka-assembly-0.0.1-SNAPSHOT.jar