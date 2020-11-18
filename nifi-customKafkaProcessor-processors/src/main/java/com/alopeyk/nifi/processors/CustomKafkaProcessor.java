package com.alopeyk.nifi.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.NoWrappingJsonEncoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.*;
import java.util.*;

@Tags({"customKafkaProcessor"})
@CapabilityDescription("Consume avro files of kafka and produces json")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class CustomKafkaProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("REL_SUCCESS")
            .description("A Flowfile is routed to this relationship when everything goes well here")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("REL_FAILURE")
            .description("A Flowfile is routed to this relationship it can not be parsed or a problem happens")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        /*if (flowFile == null) {
            return;
        }*/

        KafkaConsumer<GenericRecord, GenericData.Record> consumer = createConsumer();

        while (true) {
            ConsumerRecords<GenericRecord, GenericData.Record> records = consumer.poll(1000);
            records.forEach(record -> {
                FlowFile flowFile = session.get();
                try {
                    JsonAvroConverter jsonAvroConverter = new JsonAvroConverter();
                    byte[] jsonFile = jsonAvroConverter.convertToJson(record.value());
                    getLogger().error(new String(jsonFile));
                    flowFile = session.write(flowFile, outputStream -> {
                        outputStream.write(jsonFile);
                    });
                    getLogger().error(session.toString());
                    getLogger().error(flowFile.toString());
                    flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
                } catch (Exception e){
                    getLogger().error("Error happened");
                    session.transfer(flowFile, REL_FAILURE);
                }
                session.transfer(flowFile, REL_SUCCESS);
            });
            consumer.commitSync();
        }
    }

    private static KafkaConsumer<GenericRecord, GenericData.Record> createConsumer() {
        KafkaConsumer<GenericRecord, GenericData.Record> consumer = new KafkaConsumer<>(configConsumerProperties());
        consumer.subscribe(Collections.singletonList("mysql.alopeyk.orders"));
        return consumer;
    }

    private static Properties configConsumerProperties() {

        Properties consumerProperties = new Properties();

        consumerProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://172.16.2.220:8081");
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://172.16.2.231:9092," +
                "http://172.16.2.232:9092,http://172.16.2.220:9092,http://172.16.2.221:9092," +
                "http://172.16.2.205:9092,http://172.16.2.219:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "nifiKafkaConsumerTEST");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return consumerProperties;
    }

}
