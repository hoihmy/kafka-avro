package com.hmy;

import JavaSessionize.avro.Customer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class AvroProducer {
    private static final String TOPIC_NAME = "avro-customer-topic";
    private static final String KAFKA_SERVER_ADDRESS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_SERVER_URL = "http://localhost:8081";

    public static void main(String[] args) {
        // Kafka Producer Configurations
        final Properties kafkaProps = getKafkaAvroProducerConfiguration();

        // Create producer
        try (final Producer<String, Customer> producer = new KafkaProducer<>(kafkaProps)) {
            // Publishing The Messages
            for (int c = 0; c < 10; c++) {
                final Customer customer = CustomerGenerator.getNext();
                // Produce Avro record to Kafka
                final ProducerRecord<String, Customer> record = new ProducerRecord<>(TOPIC_NAME, "customer", customer);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        } else {
                            System.out.println("Produced message: topic = " + recordMetadata.topic() +
                                    ", partition = " + recordMetadata.partition() +
                                    ", offset = " + recordMetadata.offset());
                        }
                    }
                });
                System.out.println("Published message for customer: " + customer);
            }
        }
    }

    private static Properties getKafkaAvroProducerConfiguration() {
        final Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", KAFKA_SERVER_ADDRESS);
        kafkaProps.put("key.serializer", KafkaAvroSerializer.class);
        kafkaProps.put("value.serializer", KafkaAvroSerializer.class);
        kafkaProps.put("schema.registry.url", SCHEMA_REGISTRY_SERVER_URL);
        return kafkaProps;
    }
}