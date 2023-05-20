package com.hmy;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Producer for Avro-Encoded Kafka Messages.
 */
public class KafkaAvroMessageProducer {
    private static final String TOPIC_NAME = "avro-topic";
    private static final String KAFKA_SERVER_ADDRESS = "localhost:9092";
    private static final String AVRO_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    private static final String SCHEMA_REGISTRY_SERVER_URL = "http://localhost:8081";

    public static void main(final String[] args) throws JsonMappingException {
        System.out.println("KafkaAvroMessageProducer class");
        // Kafka Producer Configurations
        final Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", KAFKA_SERVER_ADDRESS);
        kafkaProps.put("key.serializer", KafkaAvroSerializer.class);
        kafkaProps.put("value.serializer", KafkaAvroSerializer.class);
        kafkaProps.put("schema.registry.url", SCHEMA_REGISTRY_SERVER_URL);

        // Schema Generation For Our Customer Class
        final AvroMapper avroMapper = new AvroMapper();
        final AvroSchema schema = avroMapper.schemaFor(Customer.class);

        // Create producer
        try (final Producer<String, GenericRecord> producer = new KafkaProducer<>(kafkaProps)) {
            // Publishing The Messages
            for (int c = 0; c < 10; c++) {
                final Customer customer = CustomerGenerator.getNext();
                GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema.getAvroSchema());
                recordBuilder.set("name", customer.getName());
                recordBuilder.set("email", customer.getEmail());
                recordBuilder.set("age", customer.getAge());
                recordBuilder.set("phoneNumber", customer.getPhoneNumber());
                // Create an Avro record
                final GenericRecord genericRecord = recordBuilder.build();
                // Produce Avro record to Kafka
                final ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(TOPIC_NAME, "customer", genericRecord);
                producer.send(producerRecord, new Callback() {
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
}
