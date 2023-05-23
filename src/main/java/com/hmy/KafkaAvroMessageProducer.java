package com.hmy;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;

/**
 * Producer for Avro-Encoded Kafka Messages.
 */
public class KafkaAvroMessageProducer {
    private static final String TOPIC_NAME = "avro-topic";
    private static final String KAFKA_SERVER_ADDRESS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_SERVER_URL = "http://localhost:8081";

    public static void main(final String[] args) throws IOException, RestClientException {
        System.out.println("KafkaAvroMessageProducer class");
        // Kafka Producer Configurations
        final Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", KAFKA_SERVER_ADDRESS);
        kafkaProps.put("key.serializer", KafkaAvroSerializer.class);
        kafkaProps.put("value.serializer", KafkaAvroSerializer.class);
        kafkaProps.put("schema.registry.url", SCHEMA_REGISTRY_SERVER_URL);

        // Schema Generation For Our Customer Class
//        final AvroMapper avroMapper = new AvroMapper();
//        final AvroSchema schema = avroMapper.schemaFor(Customer.class);

        // Create producer
        try (final Producer<String, GenericRecord> producer = new KafkaProducer<>(kafkaProps)) {
            // Publishing The Messages
            for (int c = 0; c < 10; c++) {
                // Create an Avro record
//                Schema.Parser parser = new Schema.Parser();
//                Schema initialSchema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Customer\",\"namespace\":\"com.hmy\",\"fields\":[{\"name\":\"age\",\"type\":{\"type\":\"int\",\"java-class\":\"java.lang.Integer\"}},{\"name\":\"email\",\"type\":[\"null\",\"string\"]},{\"name\":\"name\",\"type\":[\"null\",\"string\"]},{\"name\":\"phoneNumber\",\"type\":[\"null\",\"string\"]}]}");

                // Set the subject for which you want to retrieve the schema
                String subject = "avro-topic-value";

                // Create a SchemaRegistryClient instance
                CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_SERVER_URL, 100);

                // Fetch the existing schema from the Schema Registry
                Schema existingSchema = Schema.parse(schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema());
                System.out.println("Existing schema ID: " + schemaRegistryClient.register(subject, existingSchema));

                // Parse the existing schema
                Schema.Parser existingSchemaParser = new Schema.Parser();
                Schema existingAvroSchema = existingSchemaParser.parse(existingSchema.toString());

                // Create a new field for the schema update
                Schema.Field newField = new Schema.Field("gender", SchemaBuilder.builder().stringType(), null, null);

                // Create the updated schema with the new field
                Schema updatedSchema = SchemaBuilder
                        .record(existingAvroSchema.getName())
                        .namespace(existingAvroSchema.getNamespace())
                        .fields()
                        .name(existingAvroSchema.getFields().get(0).name())
                        .type(existingAvroSchema.getFields().get(0).schema())
                        .noDefault()
                        .name(existingAvroSchema.getFields().get(1).name())
                        .type(existingAvroSchema.getFields().get(1).schema())
                        .noDefault()
                        .name(existingAvroSchema.getFields().get(2).name())
                        .type(existingAvroSchema.getFields().get(2).schema())
                        .noDefault()
                        .name(existingAvroSchema.getFields().get(3).name())
                        .type(existingAvroSchema.getFields().get(3).schema())
                        .noDefault()
                        .name(newField.name())
                        .type(newField.schema())
                        .noDefault()
                        .endRecord();

                // Update the schema in the Schema Registry
                int newSchemaId = schemaRegistryClient.register(subject, updatedSchema);
                System.out.println("Updated schema ID: " + newSchemaId);

                // Retrieve the schema using the schema ID
                Schema newSchema = schemaRegistryClient.getByID(newSchemaId);
//                GenericRecord genericRecord = new GenericData.Record(newSchema);
//                GenericRecord genericRecord = new GenericRecordBuilder(newSchema).build();
                // Produce Avro record to Kafka
//                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(TOPIC_NAME, "customer", genericRecord);

                final Customer customer = CustomerGenerator.getNext();
                GenericRecordBuilder recordBuilder = new GenericRecordBuilder(newSchema);
                recordBuilder.set("name", customer.getName());
                recordBuilder.set("email", customer.getEmail());
                recordBuilder.set("age", customer.getAge());
                recordBuilder.set("phoneNumber", customer.getPhoneNumber());
                recordBuilder.set("gender", customer.getGender());
//                // Create an Avro record
                final GenericRecord genericRecord = recordBuilder.build();
//                // Produce Avro record to Kafka
                final ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(TOPIC_NAME, "customer", genericRecord);
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
                System.out.println("Published message for customer: " + genericRecord);
            }
        }
    }
}
