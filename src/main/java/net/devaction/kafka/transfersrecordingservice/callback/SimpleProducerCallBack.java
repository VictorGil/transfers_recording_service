package net.devaction.kafka.transfersrecordingservice.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class SimpleProducerCallBack implements Callback {
    private static final Logger log = LoggerFactory.getLogger(SimpleProducerCallBack.class);

    @Override
    public void onCompletion(final RecordMetadata metadata, final Exception exception) {
        if (exception != null) {
            log.error(" {}", exception,  exception);
            log.error("Record metadata: {}", metadata);
        } else {
            log.debug("Record metadata: {}", metadata);
        }
    }
}


