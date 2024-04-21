import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Properties;

public class Helper {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static AdminClient adminClient;
    private static Producer<byte[], byte[]> dlqProducer;

    public static AdminClient getAdminClient(){
        if (adminClient == null){
            LOG.info("Creating the Admin Client");
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_HOST_AND_TCP_PORT);
            adminClient = AdminClient.create(config);
        }
        return adminClient;
    }

    public static Producer<byte[], byte[]> getDlqProducer(){
        if(dlqProducer==null){
            dlqProducer = new KafkaProducer<>(Helper.getDlqProducerProperties());
        }
        return dlqProducer;
    }

    public static final Properties getKafkaStreamsProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "nope");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_HOST_AND_TCP_PORT);
        // props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndFailExceptionHandler.class);
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, SendToDlqProductionExceptionHandler.class.getName());
       props.put(StreamsConfig.RETRIES_CONFIG, "3");
       props.put(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, "1000");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, SendToDeadLetterQueueExceptionHandler.class.getName());
        // Override the Producer to make the failure happen much faster
        // delivery.timeout.ms should be equal to or larger than linger.ms + request.timeout.ms
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "1500");
        // streamsSettings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
        // props.put(StreamsConfig.)
       // props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String());
       // props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String());
        return props;
    }

    public static final Properties getProducerProperties() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_HOST_AND_TCP_PORT);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static final Properties getDlqProducerProperties() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_HOST_AND_TCP_PORT);

        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return props;
    }

    public static void produceMessage(Producer producer, String topic, String key, String value) throws Exception {
        producer.send(
                new ProducerRecord<>(topic, key, value),
                (event, ex) -> {
                    if (ex != null) {
                        LOG.error("Hit a problem: " + ex.getClass().getName(), ex);
                        try {
                            throw ex;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        LOG.info(String.format("Produced key %s and message %s to topic %s ", key, value, topic));
                }});
    }

    public static void produceThreeMessages(Producer producer, String topic, String key, String value) {
        final Long numMessages = 3L;

        for (Long i = 0L; i < numMessages; i++) {
            producer.send(
                    new ProducerRecord<>(topic, key, value),
                    (event, ex) -> {
                        if (ex != null)
                            LOG.error("Hit a problem: "+ex.getClass().getName(),ex);
                        else
                            LOG.info(String.format("Produced key %s and message %s to topic %s ", key, value, topic));
                    });
        }
    }

}
