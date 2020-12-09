package com.alopeyk.nifi.processors;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.*;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
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
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

@Tags({"customKafkaProcessor"})
@CapabilityDescription("Consume avro files of kafka and produces json")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class CustomKafkaProcessor extends AbstractProcessor {

    Consumer<byte[], byte[]> consumer;
    private static final byte[] EMPTY_JSON_OBJECT = "{}".getBytes(StandardCharsets.UTF_8);

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("REL_SUCCESS")
            .description("A Flowfile is routed to this relationship when everything goes well here")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("REL_FAILURE")
            .description("A Flowfile is routed to this relationship it can not be parsed or a problem happens")
            .build();

    public static final PropertyDescriptor TOPICS = new PropertyDescriptor.Builder()
            .name("Topics").description("Topic names. Accepts String or regular expression.")
            .displayName("Topics")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
            .name("schemaRegistry").description("Kafka Schema registry url")
            .displayName("Schema Registry")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BOOTSTRAP_SERVER = new PropertyDescriptor.Builder()
            .name("bootstrapServer").description("Kafka bootstrap server url. Accepts one or a comma " +
                    "separated list of addresses").displayName("Bootstrap Server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name("groupId").description("Kafka consumer group id")
            .displayName("Group ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final AllowableValue TOPIC_NAME = new AllowableValue("names", "names", "Topic is a full topic name or comma separated list of names");

    static final AllowableValue TOPIC_PATTERN = new AllowableValue("pattern", "pattern", "Topic is a regex using the Java Pattern syntax");

    static final PropertyDescriptor TOPIC_TYPE = new PropertyDescriptor.Builder()
            .name("topic_type")
            .displayName("Topic Name Format")
            .description("Specifies whether the Topic(s) provided are a comma separated list of names or a single regular expression")
            .required(true)
            .allowableValues(TOPIC_NAME, TOPIC_PATTERN)
            .defaultValue(TOPIC_NAME.getValue())
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(TOPICS);
        descriptors.add(TOPIC_TYPE);
        descriptors.add(SCHEMA_REGISTRY);
        descriptors.add(BOOTSTRAP_SERVER);
        descriptors.add(GROUP_ID);
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

        final GenericData genericData = GenericData.get();
        consumer = createKafkaConsumer(context);
        try{
            final ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
            records.forEach(record -> {
                FlowFile flowFile = session.create();
                if (flowFile == null) {
                    return;
                }
                try {
                    //whole avro recrod in text
                    //byte[] outputBytes = (record == null) ? EMPTY_JSON_OBJECT : genericData.toString(record).getBytes(StandardCharsets.UTF_8);
                    byte[] outputBytes = (record == null) ? EMPTY_JSON_OBJECT : genericData.toString(record.value()).getBytes(StandardCharsets.UTF_8);
                    flowFile = session.write(flowFile, rawOut -> {
                        //final OutputStream out = new BufferedOutputStream(rawOut);
                        rawOut.write(outputBytes);
                        consumer.commitSync();
                    });
                }catch (ProcessException pe) {
                    getLogger().error("Failed to deserialize {}", new Object[]{flowFile, pe});
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
                flowFile = session.putAttribute(flowFile,"topic",record.topic());
                flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
                session.transfer(flowFile, REL_SUCCESS);
            });
        }catch (Exception e) {
            getLogger().error(e.getMessage());
            e.printStackTrace();
            return;
        }
    }

    @OnStopped
    public void close(){
        consumer.close();
    }

    public Consumer<byte[],byte[]> createKafkaConsumer(final ProcessContext context){
        final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(configConsumerProperties(context));
        if(context.getProperty(TOPIC_TYPE).getValue().equalsIgnoreCase("pattern"))
            consumer.subscribe(Pattern.compile(context.getProperty(TOPICS).getValue()));
        if(context.getProperty(TOPIC_TYPE).getValue().equalsIgnoreCase("names"))
            consumer.subscribe(Arrays.asList(context.getProperty(TOPICS).getValue().split(",")));
        return consumer;
    }

    private static Properties configConsumerProperties(final ProcessContext context) {

        Properties consumerProperties = new Properties();

        consumerProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                context.getProperty(SCHEMA_REGISTRY).getValue());
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                context.getProperty(BOOTSTRAP_SERVER).getValue());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, context.getProperty(GROUP_ID).getValue());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        //TODO:can become dynamic based on properties as well
        //consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return consumerProperties;
    }

}
