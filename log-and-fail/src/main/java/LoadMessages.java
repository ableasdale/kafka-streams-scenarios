import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class LoadMessages {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) {

        //** Admin API Stuff here **//
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_HOST_AND_TCP_PORT);
        AdminClient admin = AdminClient.create(config);
        //creating new topic
        System.out.println("-- creating --");
        NewTopic newTopic = new NewTopic(Config.SOURCE_TOPIC, 1, (short) 1);
        admin.createTopics(Collections.singleton(newTopic));

        // ATTEMPT TWO
        Map<String, String> configMap = new HashMap<>();
        //configMap.put("cleanup.policy", "compact");
        configMap.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1024");

        String topicname = Config.SOURCE_TOPIC+"UU";

        NewTopic newTopic2 = new NewTopic(topicname, 1, (short) 1)
                .configs(configMap);
        admin.createTopics(Collections.singleton(newTopic2));

        //listing
        System.out.println("-- listing --");
        try {
            admin.listTopics().names().get().forEach(LOG::info);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        LOG.info("1. Producing to Kafka Cluster");


        Producer<String, String> producer = new KafkaProducer<>(ProducerHelper.PRODUCER_CONFIGURATION());

        final Long numMessages = 10L;

        for (Long i = 0L; i < numMessages; i++) {
            producer.send(
                    new ProducerRecord<>(topicname, "k",Config.MESSAGE_1024_BYTES),
                    (event, ex) -> {
                        if (ex != null)
                            ex.printStackTrace();
                        else
                            LOG.info(String.format("Produced event to topic %s ", topicname));
                    });
        }

        producer.flush();
        LOG.info(String.format("%s events were produced to topic %s%n", numMessages, topicname));
        producer.close();

        LOG.info("2. Creating an oversized message to Kafka Cluster");
    }
}
