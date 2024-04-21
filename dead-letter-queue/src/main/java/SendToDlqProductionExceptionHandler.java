import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

public class SendToDlqProductionExceptionHandler implements ProductionExceptionHandler {

    Producer<byte[], byte[]> dlqProducer;
    String dlqTopic;

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
    @Override
    public DeserializationExceptionHandler.DeserializationHandlerResponse handle(final ProcessorContext context,
                                                                                 final ConsumerRecord<byte[], byte[]> record,
                                                                                 final Exception exception) {

        LOG.warn("Exception caught during Deserialization, sending to the dead queue topic; " +
                        "taskId: {}, topic: {}, partition: {}, offset: {}",
                context.taskId(), record.topic(), record.partition(), record.offset(),
                exception);

        dlqProducer.send(new ProducerRecord<>(dlqTopic, null, record.timestamp(), record.key(), record.value()));

        return DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE;
    } **/

    @Override
    public void configure(Map<String, ?> map) {
        dlqProducer = Helper.getDlqProducer();
        dlqTopic = Config.DEAD_LETTER_QUEUE_TOPIC;
    }

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> producerRecord, Exception e) {
        if (e instanceof RecordTooLargeException) {
            LOG.info("FLIPPIN HECK - WE'RE HERE!!");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            return ProductionExceptionHandlerResponse.CONTINUE;
        }
        return ProductionExceptionHandlerResponse.FAIL;

    }
}
