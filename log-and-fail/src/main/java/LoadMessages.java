import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class LoadMessages {

    private static final Logger LOG = LoggerFactory.getLogger(java.lang.invoke.MethodHandles.lookup().lookupClass());

    public static void main(String[] args) {

        //** Admin API Stuff here **//
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(config);
        //creating new topic
        System.out.println("-- creating --");
        NewTopic newTopic = new NewTopic("xxxxxx", 1, (short) 1);
        admin.createTopics(Collections.singleton(newTopic));

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

        LOG.info("2. Creating an oversized message to Kafka Cluster");
    }
}
