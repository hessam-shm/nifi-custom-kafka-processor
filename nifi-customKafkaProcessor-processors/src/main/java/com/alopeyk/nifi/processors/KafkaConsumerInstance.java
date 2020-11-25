package com.alopeyk.nifi.processors;

import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerInstance {

    //TODO:must support pattern
    private final ProcessSession session;
    private final String topic;
    private final Map<String,Object> properties;
    private static final byte[] EMPTY_JSON_OBJECT = "{}".getBytes(StandardCharsets.UTF_8);

    public KafkaConsumerInstance(String topic, Map<String, String> properties, ProcessSession session) {
        this.topic = topic;
        this.properties = Collections.unmodifiableMap(properties);
        this.session = session;
    }

    public void startConsuming(){
        final GenericData genericData = GenericData.get();
        Consumer<byte[], byte[]> consumer = createKafkaConsumer();
        consumer.subscribe(Collections.singletonList(topic));
        final ConsumerRecords<byte[], byte[]> records = createKafkaConsumer().poll(1000);
        records.forEach(record -> {
                byte[] outputBytes = (record == null) ? EMPTY_JSON_OBJECT : genericData.toString(record).getBytes(StandardCharsets.UTF_8);
                FlowFile flowFile = session.get();
                flowFile = session.write(flowFile, (rawIn, rawOut) -> {
                    final OutputStream out = new BufferedOutputStream(rawOut);
                    out.write(outputBytes);
                });
        });
    }

    public Consumer<byte[],byte[]> createKafkaConsumer(){
        final Map<String,Object> consumerProperties = new HashMap<>(properties);
        //final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);
        final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);
        return consumer;
    }

}