import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class LoadMessages {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) throws Exception {

        LOG.info("Creating source topic: "+Config.SOURCE_TOPIC);
        NewTopic sourceTopic = new NewTopic(Config.SOURCE_TOPIC, 1, (short) 1);
        Helper.getAdminClient().createTopics(Collections.singleton(sourceTopic));

        LOG.info("Creating destination topic: "+Config.DESTINATION_TOPIC);
        // We're creating this with a small max.message.bytes to trigger an exception when Kafka Streams runs..
        Map<String, String> configMap = new HashMap<>();
        //configMap.put("cleanup.policy", "compact");
        configMap.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "96");
        NewTopic destinationTopic = new NewTopic(Config.DESTINATION_TOPIC, 1, (short) 1)
                .configs(configMap);
        Helper.getAdminClient().createTopics(Collections.singleton(destinationTopic));

        LOG.info("Listing Available Topics:");
        try {
            Helper.getAdminClient().listTopics().names().get().forEach(LOG::info);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        LOG.info("1. Producing some small messages to Source topic");
        Producer<String, String> producer = new KafkaProducer<>(Helper.getProducerProperties());
        // Note that MAX_MESSAGE_BYTES includes both the key and the value when determining whether the message is too large
        Helper.produceThreeMessages(producer, Config.SOURCE_TOPIC, "k", Config.MESSAGE_UNDER_96_BYTES);
        producer.flush();
        LOG.info("2. Producing oversize message to Source topic (this will be fine)");
        Helper.produceMessage(producer, Config.SOURCE_TOPIC, "k", Config.MESSAGE_EXCEEDS_96_BYTES);
        producer.flush();
        LOG.info("3. Adding more small messages to Source topic");
        Helper.produceThreeMessages(producer, Config.SOURCE_TOPIC, "k", Config.MESSAGE_UNDER_96_BYTES);
        producer.flush();
        producer.close();

        /**
         * You could use builder.stream().transform().to() and put the partition from the context into a member variable of the StreamPartitioner (ie, you also need to give a reference of the StreamPartitioner instance into the Transformer).
         */
        LOG.info("3. Creating a Kafka Streams App to demonstrate the failure scenario");
        StreamsBuilder builder = new StreamsBuilder();
        //StreamsBuilder builder = new StreamsBuilder();
        builder.stream(Config.SOURCE_TOPIC, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .to(Config.DESTINATION_TOPIC, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
        Topology topology = builder.build();

        LOG.info("Top:"+topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, Helper.getKafkaStreamsProperties());
        streams.setUncaughtExceptionHandler((exception) -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        //streams.setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        streams.start();
    }

    private static Object calculatePartition(Object topicName, Object key, Object value, Object numberOfPartitions) {
        LOG.info("what to do?");
        return null;
    }
}
