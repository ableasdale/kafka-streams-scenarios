import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class ProducerHelper {

    public static final Properties PRODUCER_CONFIGURATION() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_HOST_AND_TCP_PORT);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

}
