import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

public class StreamsCustomUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        LOG.info("YO YO YO YO YO!!");
        if (exception instanceof StreamsException) {
            Throwable originalException = exception.getCause();
            if (originalException.getMessage().equals("Retryable transient error")) {
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        }
        return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }
}
